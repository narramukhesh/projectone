from projectoneflow.core.types import C
from typing import Type
from projectoneflow.framework.exception.contract import ContractNotDefined
from projectoneflow.core.schemas import ParentEnum
from typing import Protocol, runtime_checkable


class ContractType(ParentEnum):
    """This class is defined to specify the contract types"""

    project = "project"


@runtime_checkable
class Contract(Protocol):
    """This is interface definition where child contract type will have implementation"""

    def get_contract(contract_name: ContractType) -> Type[C]:
        """
        This method returns the contact object for the specific contract

        Parameters
        -------------------
        contract_name: str
            This is the contract name to which contract object is created

        Returns
        -------------------
        Type[C]
            Contract object specific to contract name
        """

        if contract_name == ContractType.project:
            from projectoneflow.framework.contract.project import ProjectContract

            return ProjectContract
        else:
            raise ContractNotDefined(
                f"Provided {contract_name} contract is not Implemented"
            )
