from __future__ import absolute_import

from certmaster import certmaster

from grokcore.component import context, subscribe
from twisted.internet import defer

from opennode.knot.backend.compute import format_error
from opennode.knot.model.compute import ICompute, IVirtualCompute
from opennode.knot.model.machines import IIncomingMachineRequest, IncomingMachineRequest
from opennode.knot.model.compute import IFuncInstalled
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import IModelDeletedEvent, IModelModifiedEvent
from opennode.oms.model.model.actions import Action, action
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db


class AcceptHostRequestAction(Action):
    """Accept request of the host for joining OMS/certmaster"""
    context(IIncomingMachineRequest)

    action('accept')

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            cm = certmaster.CertMaster()
            yield cm.sign_this_csr("%s.csr" % self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


class RejectHostRequestAction(Action):
    """Remove request of the host for joining OMS/certmaster"""
    context(IIncomingMachineRequest)

    action('reject')

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            cm = certmaster.CertMaster()
            yield cm.remove_this_cert(self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))



@subscribe(ICompute, IModelDeletedEvent)
def delete_compute(model, event):
    if IFuncInstalled.providedBy(model):
        blocking_yield(RejectHostRequestAction(
            IncomingMachineRequest(model.hostname)).execute(DetachedProtocol(), object()))


@subscribe(IVirtualCompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_virtual_compute_config_change_request(compute, event):
    update_param_whitelist = ['cpu_limit',
                              'memory',
                              'num_cores',
                              'swap_size']

    params_to_update = filter(lambda k,v: k in update_param_whitelist, event.modified.iteritems())

    if len(params_to_update) == 0:
        return

    update_values = [v for k,v in sorted(params_to_update, key=lambda k,v: k)]

    submitter = IVirtualizationContainerSubmitter(compute.__parent__)
    try:
        yield submitter.submit(IUpdateVM, compute.__name__, *update_values)
    except Exception:
        for mk, mv in event.modified.iteritems():
            setattr(compute, mk, event.original[mk])
        raise
