from twisted.internet import defer
from zope.component import provideSubscriptionAdapter, queryAdapter
from zope.interface import implements, Interface

from opennode.oms.config import get_config
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.util import subscription_factory, async_sleep
from opennode.oms.zodb import db
from opennode.oms.model.model.symlink import follow_symlinks


class IMetricsGatherer(Interface):
    def gather():
        """Gathers metrics for some object"""


class MetricsDaemonProcess(DaemonProcess):
    implements(IProcess)

    __name__ = "metrics"

    def __init__(self):
        super(MetricsDaemonProcess, self).__init__()

        config = get_config()
        self.interval = config.getint('metrics', 'interval')

    @defer.inlineCallbacks
    def run(self):
        while True:
            yield async_sleep(self.interval)
            try:
                # Currently we have special codes for gathering info about machines
                # hostinginv VM, in future here we'll traverse the whole zodb and search for gatherers
                # and maintain the gatherers via add/remove events.
                if not self.paused:
                    yield self.gather_machines()
            except Exception:
                import traceback
                traceback.print_exc()
                pass

    def log(self, msg):
        print "[metrics] %s" % (msg, )

    @defer.inlineCallbacks
    def gather_machines(self):
        @db.ro_transact
        def get_gatherers():
            res = []

            oms_root = db.get_root()['oms_root']
            for i in [follow_symlinks(i) for i in oms_root['computes'].listcontent()]:
                adapter = queryAdapter(i, IMetricsGatherer)
                if adapter:
                    res.append(adapter)

            return res

        for i in (yield get_gatherers()):
            try:
                yield i.gather()
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.log("Got exception when gathering metrics compute '%s': %s" % (i.context, e))


provideSubscriptionAdapter(subscription_factory(MetricsDaemonProcess), adapts=(Proc,))
