from datetime import datetime, timedelta
from grokcore.component import context, implements, name, GlobalUtility
from twisted.internet import defer
from twisted.enterprise import adbapi
from zope.interface import Interface
from zope.component import getUtility

import logging
import sys

from opennode.knot.model.compute import IVirtualCompute
from opennode.knot.model.user import IUserStatisticsLogger

from opennode.oms.config import get_config
from opennode.oms.model.model.hooks import IPreValidateHook
from opennode.oms.model.traversal import traverse1
from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)
    name('user-stats-logger')

    def __init__(self):
        self.slog = logging.getLogger("%s.userstats" % __name__)

    def log(self, user, stats_data):
        data = {'username': user, 'stats': stats_data}
        self.slog.info('', extra=data)


class SqlDBUserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)
    name('user-stats-sqldb-logger')

    @defer.inlineCallbacks
    def initdb(self):
        op = get_config().getstring('stats', 'db_init')
        try:
            yield self._db.runOperation(op)
        except Exception:
            log.error('', exc_info=sys.exc_info())

    def config(self):
        self.db_backend = get_config().getstring('stats', 'db_backend', 'sqlite3')
        self.db_conn_param = get_config().getstring('stats', 'db_conn_param', ':memory:').split(';')
        self.db_conn_kw = eval(get_config().getstring('stats', 'db_conn_kw', '{}'))
        self.db_operation = get_config().getstring('stats', 'db_operation',
                                                   'INSERT INTO CONF_CHANGES (username, timestamp, cores,'
                                                   'disk, memory, number_of_vms, last_known_credit) '
                                                   'VALUES (%s, %s, %s, %s, %s, %s, %s)')

        self._db = adbapi.ConnectionPool(self.db_backend, *self.db_conn_param, **self.db_conn_kw)

        if get_config().getstring('stats', 'db_init', None):
            return self.initdb()

        return defer.succeed(None)

    @defer.inlineCallbacks
    def log(self, user, stats_data):
        try:
            if not hasattr(self, '_db'):
                yield self.config()

            yield self._db.runOperation(self.db_operation,
                                        (user, stats_data['timestamp'], stats_data['num_cores_total'],
                                         stats_data['diskspace_total'], stats_data['memory_total'],
                                         stats_data['vm_count'], stats_data['credit']))
        except Exception:
            log.error('DB error', exc_info=sys.exc_info())


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    context(IVirtualCompute)
    name('user-credit-check')

    @defer.inlineCallbacks
    def apply(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        credit_check_cooldown = get_config().getstring('auth', 'billing_timeout', 60)

        @db.ro_transact
        def get_profile_and_need_update():
            try:
                profile = traverse1('/home/%s' % principal.id)
                timeout = (datetime.strptime(profile.credit_timestamp, '%Y-%m-%dT%H:%M:%S.%f') +
                           timedelta(seconds=credit_check_cooldown))
                log.debug('Next update for "%s": %s', principal.id, timeout)
                return (profile, profile.uid, timeout < datetime.now())
            except Exception as e:
                log.error('%s', e)
                raise

        @db.transact
        def update_credit(credit):
            profile = traverse1('/home/%s' % principal.id)
            profile.credit = credit
            log.debug('Updated credit of %s: %s', profile, profile.credit)
            return profile

        if billable_group in map(str, principal.groups):
            profile, uid, need_update = yield get_profile_and_need_update()
            log.debug('%s (uid=%s) need_update: %s', profile, uid, need_update)

            if need_update:
                try:
                    check_call = getUtility(ICreditCheckCall)
                    credit = yield defer.maybeDeferred(check_call.get_credit, uid)
                    profile = yield update_credit(credit)
                except Exception as e:
                    log.error('Error updating credit: %s', e, exc_info=sys.exc_info())

            @db.ro_transact()
            def check_credit(profile):
                assert profile.has_credit(), ('User %s does not have enough credit' % principal.id)

            yield check_credit(profile)
        else:
            log.info('User is not a member of a billable group "%s": %s. Not updating credit.',
                     billable_group, map(str, principal.groups))

    def applicable(self, context):
        return IVirtualCompute.providedBy(context)


class ICreditCheckCall(Interface):

    def get_credit(self, uid):
        """ Get credit """
