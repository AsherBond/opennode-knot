from __future__ import absolute_import

from certmaster import certmaster

from grokcore.component import context

from opennode.knot.model import BaseIncomingMachines, IncomingMachines
from opennode.oms.model.model.base import  ContainerInjector


class IncomingMachinesCertmaster(BaseIncomingMachines):
    __name__ = 'certmaster'

    def _get(self):
        cm = certmaster.CertMaster()
        return cm.get_csrs_waiting()

class IncomingMachinesFuncInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesCertmaster
