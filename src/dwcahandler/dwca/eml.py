
from dataclasses import dataclass, field
import metapype.eml.export
from metapype.eml import names
import metapype.model.metapype_io
from metapype.model.node import Node


@dataclass
class Eml:

    package_id: str = field(default='ala')
    system: str = field(default='galaxias-python')
    dataset_name: str = field(default='')
    description: str = field(default='')
    citation: str = field(default='')
    license: str = field(default='')
    rights: str = field(default='')

    def build_eml_xml(self):
        # Write EML XML
        eml = Node(names.EML)
        eml.add_attribute('packageId', self.package_id)
        eml.add_attribute('system', self.system)

        dataset = Node(names.DATASET, parent=eml)
        eml.add_child(dataset)

        title = Node(names.TITLE, parent=dataset)
        title.content = self.dataset_name
        dataset.add_child(title)

        abstract = Node(names.ABSTRACT, parent=eml)
        abstract.content = self.description
        eml.add_child(abstract)

        intellectual_rights = Node(names.INTELLECTUALRIGHTS, parent=dataset)
        eml.add_child(intellectual_rights)

        para = Node(names.PARA, parent=intellectual_rights, content=self.rights)
        para.content = f"{self.rights}"
        intellectual_rights.add_child(para)

        xml_str = metapype.eml.export.to_xml(eml)
        return xml_str
