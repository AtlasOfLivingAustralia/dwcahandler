"""
This module provides dataclasses and helper methods for constructing and validating EML (Ecological Metadata Language) XML documents using the metapype library.

Key Features:
- BaseElements: Base class for converting dataclass fields into EML XML nodes, supporting nested and list structures.
- Data Model Classes: Name, Address, Contact, Description, Coverage, Dataset, etc., each mapping to EML elements via field metadata.
- Eml: Top-level class for assembling and validating the EML XML tree.

Usage:
- Instantiate the dataclasses to represent EML metadata.
- Use the Eml class and its build_eml_xml() method to generate and validate the EML XML string.

Logging:
- Validation errors are logged using Python's logging module.
"""
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
    """
    Base class for EML dataclasses. Provides methods to convert dataclass fields into EML XML nodes,
    supporting nested and list structures for EML document construction.
    """
    def get_class_fields(self):
        """
        Returns a list of dataclass fields for the current instance.
        Used to introspect the dataclass for EML element mapping.
        """
        return fields(globals()[self.__class__.__name__])

    def make_node(self, node_name: str, parent_node: Node, content_value: str = None):
        """
        Creates an EML node with the given name and content, and attaches it to the parent node.
        If content_value is None, creates an empty node.
        """
        node = Node(node_name, parent=parent_node)
        if content_value:
            node.content = content_value
        return node

    def make_children_node(self, child_obj, node_name: str, parent_node: Node):
        """
        Recursively creates child EML nodes from a dataclass or list of dataclasses and attaches them to the parent node.
        Handles both single objects and lists.
        """
        node = child_obj.make_node(node_name=node_name, parent_node=parent_node)
        child_node = child_obj.make_node_from_elements(parent_node=node)
        parent_node.add_child(child_node)
        return parent_node

    def make_node_from_elements(self, parent_node: Node):
        """
        Converts all dataclass fields into EML nodes and attaches them to the parent node.
        Handles nested dataclasses and lists for complex EML structures.
        """
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
    """
    Represents an individual's name in EML, mapping to given name and surname elements.
    """
    first_name: str = field(default=None, metadata={EML_ELM_MAPPING: names.GIVENNAME})
    last_name: str = field(default=None, metadata={EML_ELM_MAPPING: names.SURNAME})


@dataclass
class Address(BaseElements):
    """
    Represents a postal address in EML, including delivery point, city, administrative area, postal code, and country.
    """
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
    """
    Represents a contact or party in EML, including name, organization, position, address, phone, email, and user ID.
    """
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
    """
    Represents a descriptive text element in EML, such as abstract or citation.
    """
    description: str = field(default=None, metadata={EML_ELM_MAPPING: names.PARA})


@dataclass
class BoundingCoordinates(BaseElements):
    """
    Represents the bounding coordinates (west, east, north, south) for geographic coverage in EML.
    """
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
    """
    Represents the geographic coverage section in EML, including description and bounding coordinates.
    """
    description: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.GEOGRAPHICDESCRIPTION}
    )
    bounding_coordinates: BoundingCoordinates = field(
        default=None, metadata={EML_ELM_MAPPING: names.BOUNDINGCOORDINATES}
    )


@dataclass
class CalendarDate(BaseElements):
    """
    Represents a calendar date in EML, used for temporal coverage.
    """
    calendar_date: str = field(default=None, metadata={EML_ELM_MAPPING: names.CALENDARDATE})


@dataclass
class DateRange(BaseElements):
    """
    Represents a date range in EML, with begin and end dates for temporal coverage.
    """
    begin_date: CalendarDate = field(default=None, metadata={EML_ELM_MAPPING: names.BEGINDATE})
    end_date: CalendarDate = field(default=None, metadata={EML_ELM_MAPPING: names.ENDDATE})


@dataclass
class TemporalCoverage(BaseElements):
    """
    Represents the temporal coverage section in EML, specifying the range of dates covered by the dataset.
    """
    range_of_dates: DateRange = field(
        default=None, metadata={EML_ELM_MAPPING: names.RANGEOFDATES}
    )


@dataclass
class TaxonomicClassification(BaseElements):
    """
    Represents a single taxonomic classification in EML, including rank name and value.
    """
    taxon_rank_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONRANKNAME}
    )
    taxon_rank_value: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONRANKVALUE}
    )

@dataclass
class TaxonomicCoverage(BaseElements):
    """
    Represents the taxonomic coverage section in EML, including general coverage and a list of classifications.
    """
    general_taxonomic_coverage: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.GENERALTAXONOMICCOVERAGE}
    )
    taxonomic_classification: list[TaxonomicClassification] = field(
        default=None, metadata={EML_ELM_MAPPING: names.TAXONOMICCLASSIFICATION}
    )


@dataclass
class Coverage(BaseElements):
    """
    Represents the overall coverage section in EML, including geographic, temporal, and taxonomic coverage.
    """
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
    """
    Represents a set of keywords and an optional thesaurus in EML.
    """
    keyword: str = field(default=None, metadata={EML_ELM_MAPPING: names.KEYWORD})
    keyword_thesaurus: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.KEYWORDTHESAURUS}
    )


@dataclass
class Dataset(BaseElements):
    """
    Represents the main dataset section in EML, including title, identifiers, keywords, creators, abstract, rights, coverage, and contacts.
    """
    dataset_name: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.TITLE}
    )  # dataset -> title
    alternate_identifier: list[str] = field(
        default=None, metadata={EML_ELM_MAPPING: names.ALTERNATEIDENTIFIER}
    )  # dataset -> alternateIdentifiers
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
    """
    Represents additional metadata in EML, such as citation and extra information.
    """
    citation: str = field(
        default=None, metadata={EML_ELM_MAPPING: names.CITATION}
    )
    additional_info: Description = field(
        default=None, metadata={EML_ELM_MAPPING: names.ADDITIONALINFO}
    )


@dataclass
class GBIFMetadata(Metadata):
    """
    Represents GBIF-specific metadata in EML.
    """
    gbif: list[dict] = None

    # Override the make_node_from_elements to handle gbif element
    def make_node_from_elements(self, parent_node: Node):
        if not self.gbif:
            self.gbif = []
        gbif_node = self.make_node(
            node_name="gbif",
            parent_node=parent_node,
        )
        citation_field = None
        citation_fields = [f for f in self.get_class_fields() if f.name == "citation"]
        if len(citation_fields) > 0:
            citation_field = citation_fields[0]
        if citation_field:
            citation_node = self.make_node(
                node_name=self.get_class_fields()[0].metadata[EML_ELM_MAPPING],
                content_value=self.citation,
                parent_node=gbif_node,
            )
            gbif_node.add_child(citation_node)
        if self.gbif and len(self.gbif) > 0:
            for gbif_item in self.gbif:
                for key, value in gbif_item.items():
                    child_node = self.make_node(key, parent_node=gbif_node)
                    child_node.content = value
                    gbif_node.add_child(child_node)
        parent_node.add_child(gbif_node)
        return parent_node


@dataclass
class AdditionalMetadata(BaseElements):
    """
    Represents the additionalMetadata section in EML, containing extra metadata blocks.
    """
    metadata: Metadata = field(
        default=None, metadata={EML_ELM_MAPPING: names.METADATA}
    )


@dataclass
class Eml(BaseElements):
    """
    Top-level class for assembling and validating an EML XML document. Contains dataset and additional metadata.
    Use build_eml_xml() to generate and validate the EML XML string.
    """
    dataset: Dataset = field(
        default=None, metadata={EML_ELM_MAPPING: names.DATASET}
    )
    additional_metadata: AdditionalMetadata = field(
        default=None, metadata={EML_ELM_MAPPING: names.ADDITIONALMETADATA}
    )
    eml: Node = field(init=False)

    def __post_init__(self):
        """
        Initializes the Eml object by creating the root EML node and setting required attributes.
        This method is automatically called after the dataclass is initialized.
        """
        node = Node(names.EML)
        node.add_attribute("packageId", "ala")
        node.add_attribute("system", "dwcahandler")
        self.eml = node

    def build_eml_xml(self):
        """
        Builds the EML XML string from the dataclass structure and validates it.
        Returns:
            str: The generated EML XML as a string. Logs validation errors if present.
        """
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
