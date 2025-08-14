from dataclasses import dataclass, field, fields
import metapype.eml.export
from metapype.eml import names
from metapype.model.node import Node
from metapype.model import metapype_io
from metapype.eml import validate
from metapype.eml.validation_errors import ValidationError
from datetime import datetime
from xml.sax.saxutils import unescape
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.ERROR)
log = logging.getLogger("eml")

EML_ELM_MAPPING = "node_name"


@dataclass
class BaseElements:
    def get_class_fields(self):
        return fields(globals()[self.__class__.__name__])

    def make_node(self, node_name: str, parent_node: Node, content_value: str = None):
        node = Node(node_name, parent=parent_node)
        if content_value:
            node.content = content_value
        return node

    def make_children_node(self, child_obj, node_name: str, parent_node: Node):
        node = child_obj.make_node(node_name=node_name, parent_node=parent_node)
        child_node = child_obj.make_node_from_elements(parent_node=node)
        parent_node.add_child(child_node)
        return parent_node

    def make_node_from_elements(self, parent_node: Node):
        elm_fields = self.get_class_fields()
        for elm_field in elm_fields:
            node_name = (
                elm_field.metadata[EML_ELM_MAPPING]
                if len(elm_field.metadata) > 0
                else None
            )
            if node_name:
                if elm_field.type == str or elm_field.type == datetime:
                    if self.__dict__[elm_field.name]:
                        node = self.make_node(
                            node_name=node_name,
                            content_value=self.__dict__[elm_field.name],
                            parent_node=parent_node,
                        )
                        parent_node.add_child(node)
                else:
                    node_name = elm_field.metadata[EML_ELM_MAPPING]
                    if hasattr(self, elm_field.name):
                        child_obj = getattr(self, elm_field.name)
                        if isinstance(child_obj, list):
                            for obj in child_obj:
                                if isinstance(obj, str):
                                    child_node = self.make_node(
                                        node_name=node_name,
                                        content_value=obj,
                                        parent_node=parent_node,
                                    )
                                    parent_node.add_child(child_node)
                                else:
                                    parent_node = self.make_children_node(
                                        child_obj=obj,
                                        node_name=node_name,
                                        parent_node=parent_node,
                                    )
                        elif child_obj:
                            parent_node = self.make_children_node(
                                child_obj=child_obj,
                                node_name=node_name,
                                parent_node=parent_node,
                            )

        return parent_node


@dataclass
class Name(BaseElements):
    first_name: str = field(default=None, metadata={EML_ELM_MAPPING: names.GIVENNAME})
    last_name: str = field(default=None, metadata={EML_ELM_MAPPING: names.SURNAME})


@dataclass
class Address(BaseElements):
    delivery_point: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.DELIVERYPOINT}
    )
    city: str = field(default=None, metadata={EML_ELM_MAPPING: names.CITY})
    administrative_area: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.ADMINISTRATIVEAREA}
    )
    postal_code: str = field(default=None, metadata={EML_ELM_MAPPING: names.POSTALCODE})
    country: str = field(default=None, metadata={EML_ELM_MAPPING: names.COUNTRY})


@dataclass
class Contact(BaseElements):
    individual_name: Name = field(
        default=None, metadata={EML_ELM_MAPPING: names.INDIVIDUALNAME}
    )
    organization_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.ORGANIZATIONNAME}
    )
    position_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.POSITIONNAME}
    )
    address: Address = field(default=None, metadata={EML_ELM_MAPPING: names.ADDRESS})
    phone: str = field(default="", metadata={EML_ELM_MAPPING: names.PHONE})
    email: str = field(
        default="", metadata={EML_ELM_MAPPING: names.ELECTRONICMAILADDRESS}
    )
    userid: str = field(
        default="", metadata={EML_ELM_MAPPING: names.USERID}
    )  # orcid directory link


@dataclass()
class Description(BaseElements):
    description: str = field(default=None, metadata={EML_ELM_MAPPING: names.PARA})


@dataclass
class BoundingCoordinates(BaseElements):
    west: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.WESTBOUNDINGCOORDINATE}
    )
    east: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.EASTBOUNDINGCOORDINATE}
    )
    north: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.NORTHBOUNDINGCOORDINATE}
    )
    south: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.SOUTHBOUNDINGCOORDINATE}
    )


@dataclass
class GeographicCoverage(BaseElements):
    description: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.GEOGRAPHICDESCRIPTION}
    )
    bounding_coordinates: BoundingCoordinates = field(
        default=None, metadata={EML_ELM_MAPPING: names.BOUNDINGCOORDINATES}
    )


@dataclass
class CalendarDate(BaseElements):
    calendar_date: str = field(default=None, metadata={EML_ELM_MAPPING: names.CALENDARDATE})


@dataclass
class DateRange(BaseElements):
    begin_date: CalendarDate = field(default=None, metadata={EML_ELM_MAPPING: names.BEGINDATE})
    end_date: CalendarDate = field(default=None, metadata={EML_ELM_MAPPING: names.ENDDATE})


@dataclass
class TemporalCoverage(BaseElements):
    range_of_dates: DateRange = field(
        default=None, metadata={EML_ELM_MAPPING: names.RANGEOFDATES}
    )


@dataclass
class TaxonomicClassification(BaseElements):
    taxon_rank_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONRANKNAME}
    )
    taxon_rank_value: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONRANKVALUE}
    )

@dataclass
class TaxonomicCoverage(BaseElements):
    general_taxonomic_coverage: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.GENERALTAXONOMICCOVERAGE}
    )
    taxonomic_classification: list[TaxonomicClassification] = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONOMICCLASSIFICATION}
    )


@dataclass
class Coverage(BaseElements):
    geographic_coverage: GeographicCoverage = field(
        default=None, metadata={EML_ELM_MAPPING: names.GEOGRAPHICCOVERAGE}
    )
    temporal_coverage: TemporalCoverage = field(
        default=None, metadata={EML_ELM_MAPPING: names.TEMPORALCOVERAGE}
    )
    taxonomic_coverage: TaxonomicCoverage = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONOMICCOVERAGE}
    )


@dataclass
class KeywordSet(BaseElements):
    keyword: str = field(default=None, metadata={EML_ELM_MAPPING: names.KEYWORD})
    keyword_thesaurus: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.KEYWORDTHESAURUS}
    )


@dataclass
class Dataset(BaseElements):
    dataset_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TITLE}
    )  # dataset -> title
    alternate_identifier: list[str] = field(
        default=None, metadata={EML_ELM_MAPPING: names.ALTERNATEIDENTIFIER}
    )  # dataset -> title
    keyword_set: list[KeywordSet] = field(
        default=None, metadata={EML_ELM_MAPPING: names.KEYWORDSET}
    )  # dataset -> keywordSet
    creator: Contact = field(
        default=None, metadata={EML_ELM_MAPPING: names.CREATOR}
    )  # dataset -> creator
    metaProvider: Contact = field(
        default=None, metadata={EML_ELM_MAPPING: names.METADATAPROVIDER}
    )  # dataset -> metaProvider
    associatedParty: Contact = field(
        default=None, metadata={EML_ELM_MAPPING: names.ASSOCIATEDPARTY}
    )  # dataset -> associatedParty
    published_date: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.PUBDATE}
    )  # dataset -> pubDate
    language: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.LANGUAGE}
    )  # dataset -> language
    abstract: Description = field(
        default=None, metadata={EML_ELM_MAPPING: names.ABSTRACT}
    )  # dataset -> abstract
    intellectual_rights: list[Description] = field(
        default=None, metadata={EML_ELM_MAPPING: names.INTELLECTUALRIGHTS}
    )  # dataset -> intellectualRights
    coverage: Coverage = field(
        default=None, metadata={EML_ELM_MAPPING: names.COVERAGE}
    )  # dataset -> coverage
    contact: Contact = field(
        default=None, metadata={EML_ELM_MAPPING: names.CONTACT}
    )  # dataset -> contact


@dataclass
class Metadata(BaseElements):
    citation: Description = field(
        default=None, metadata={EML_ELM_MAPPING: names.CITATION}
    )
    additional_info: Description = field(
        default=None, metadata={EML_ELM_MAPPING: names.ADDITIONALINFO}
    )


@dataclass
class AdditionalMetadata(BaseElements):
    metadata: Metadata = field(
        default=None, metadata={EML_ELM_MAPPING: names.METADATA}
    )


@dataclass
class Eml(BaseElements):
    dataset: Dataset = field(
        default=None, metadata={EML_ELM_MAPPING: names.DATASET}
    )
    additional_metadata: AdditionalMetadata = field(
        default=None, metadata={EML_ELM_MAPPING: names.ADDITIONALMETADATA}
    )
    eml: Node = field(init=False)

    def __post_init__(self):
        node = Node(names.EML)
        node.add_attribute("packageId", "ala")
        node.add_attribute("system", "dwcahandler")
        self.eml = node

    def build_eml_xml(self):
        eml = self.eml
        eml = self.make_node_from_elements(parent_node=eml)
        xml_str = metapype.eml.export.to_xml(eml)
        xml_str = unescape(xml_str)

        # Validate eml string that's built
        eml_node = metapype_io.from_xml(xml_str)
        if eml_node:
            eml_errors: list = []
            validate.tree(eml_node, eml_errors)
            if len (eml_errors) > 0:
                for error in eml_errors:
                    if error[0] == ValidationError.MIN_CHOICE_UNMET:
                        logging.error(
                            "Errors with the generated eml: %s",
                            ValueError(
                                "Dataset must be defined or AdditionalMetadata is defined but it's not complete"
                            ),
                        )

        return xml_str
