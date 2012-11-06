from __future__ import absolute_import

from grokcore.component import context, subscribe
from twisted.internet import defer

from opennode.knot.backend.operation import ISaltInstalled
from opennode.knot.backend.salt.machines import RegisteredMachinesSalt
from opennode.knot.model.compute import ICompute, register_machine
from opennode.knot.model.machines import IIncomingMachineRequest, IncomingMachineRequest
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.model.actions import Action, action
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db

def format_error(e):
    return (": ".join(msg for msg in e.args if isinstance(msg, str)
                                          and not msg.startswith('  File "/')))


class AcceptHostRequestAction(Action):
    """Accept request of the host for joining OMS/Salt"""
    context(IIncomingMachineRequest)

    action('accept')

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            # TODO: implement Salt acceptance
            raise NotImplemented("TODO: implement")
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


class RejectHostRequestAction(Action):
    """Remove request of the host for joining OMS/Salt"""
    context(IIncomingMachineRequest)

    action('reject')

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            # TODO: implement Salt rejection
            raise NotImplemented("TODO: implement")
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


@defer.inlineCallbacks
def import_machines():
    accepted = RegisteredMachinesSalt()._get()
    for host in accepted:
        yield register_machine(host)


@subscribe(ICompute, IModelDeletedEvent)
def delete_compute(model, event):
    if ISaltInstalled.providedBy(model):
        blocking_yield(RejectHostRequestAction(
            IncomingMachineRequest(model.hostname)).execute(DetachedProtocol(), object()))
