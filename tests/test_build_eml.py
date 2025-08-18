import pytest
import metapype.eml.validate as validate
import metapype.model.metapype_io as metapype_io
from metapype.model.node import Node
from metapype.eml import names
from dwcahandler import (
    Name,
    Address,
    Contact,
    Description,
    BoundingCoordinates,
    GeographicCoverage,
    DateRange,
    CalendarDate,
    TemporalCoverage,
    TaxonomicClassification,
    TaxonomicCoverage,
    Coverage,
    KeywordSet,
    Dataset,
    AdditionalMetadata,
    Metadata,
    Eml,
)

EML_METADATA_NOT_COMPLETE = "Errors with the generated eml: Dataset must be defined or AdditionalMetadata is defined but it's not complete"

def extract_content_from_children(node: Node):
    children_content = {}
    if len(node.children) > 0:
        for child_node in node.children:
            children_content[child_node.name] = child_node.content
    return children_content


def compare_child_node_content(
    node: Node,
    expected_node: Node,
):
    node_children_content = extract_content_from_children(node)
    expected_children_node_content = extract_content_from_children(expected_node)
    assert node_children_content == expected_children_node_content

def compare_node_contents(node: Node, expected_node: Node, child_node_names: list[str]):
    child_node = node
    expected_child_node = expected_node
    for child_node_name in child_node_names:
        next_node = expected_node.find_child(child_node_name)
        if next_node:
            expected_child_node = next_node
            child_node = node.find_child(child_name=child_node_name)
            assert child_node
            compare_child_node_content(child_node, expected_child_node)
        else:
            compare_node_contents(child_node, expected_child_node, [child_node_name])

    return child_node, expected_child_node


class TestBuildEml:
    """
    This checks that the dwca file that is produced has the proper meta xml schema and format and
    content is as expected
    """

    @pytest.fixture()
    def dataset(self):
        contact_person: Contact = Contact(
            individual_name=Name(first_name="John", last_name="Doe"),
            address=Address(city="City", postal_code="ABC-123", country="Country"),
            organization_name="An Organization",
            email="john.doe@org.com",
            userid="https://orcid.org/0000-0000-0000-0000",
        )

        dataset = Dataset(
            dataset_name="Test Dataset",
            alternate_identifier=[
                "https://ipt/eml.do?r=a-resource",
                "https://another-website/dataset-info",
            ],
            keyword_set=[
                KeywordSet(
                    keyword="Occurrence",
                    keyword_thesaurus="http://rs.gbif.org/vocabulary/gbif/dataset_type_2015-07-10.xml",
                )
            ],
            abstract=Description(
                description="Lorem Ipsum is simply dummy text of the printing and typesetting industry. \n"
                "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when \n"
                "an unknown printer took a galley of type and scrambled it to make a type specimen book"
            ),
            creator=contact_person,
            published_date="2020-08-31",
            coverage=Coverage(
                geographic_coverage=GeographicCoverage(
                    description="The data set contains records of herbarium specimens",
                    bounding_coordinates=BoundingCoordinates(west="1.0", east="2.0", north="3.0", south="4.0"),
                ),
                temporal_coverage=TemporalCoverage(
                    DateRange(
                        begin_date=CalendarDate(calendar_date="2020-01-01"),
                        end_date=CalendarDate(calendar_date="2020-01-30")
                    )
                ),
                taxonomic_coverage=TaxonomicCoverage(
                    general_taxonomic_coverage="All vascular plants are identified as species or genus",
                    taxonomic_classification=[
                        TaxonomicClassification(taxon_rank_name="Genus", taxon_rank_value="Acacia"),
                        TaxonomicClassification(taxon_rank_name="Genus", taxon_rank_value="Acacia"),
                    ],
                ),
            ),
            intellectual_rights=[
                Description(
                    description="""This work is licensed under a 
                                    <ulink url='http://creativecommons.org/licenses/by/4.0/legalcode'> 
                                    <citetitle>Creative Commons Attribution (CC-BY) 4.0 License</citetitle></ulink>"""
                )
            ],
            contact=contact_person,
        )
        return dataset

    @pytest.fixture()
    def additional_metadata(self):
        additional_metadata = AdditionalMetadata(
                metadata=Metadata(
                    citation=Description(description="Researchers should cite this work as follows: xxxxx"),
                    additional_info=Description(
                        description="""
                            <gbif>
                                <dateStamp>2025-03-03T03:37:21</dateStamp>
                                <hierarchyLevel>dataset</hierarchyLevel>
                                <resourceLogoUrl>https://logo-url/logo.png</resourceLogoUrl>
                            </gbif>
                            """
                    ),
                )
            )

        return additional_metadata

    def test_build_eml_via_init(self, dataset, additional_metadata):
        eml = Eml(dataset=dataset, additional_metadata=additional_metadata)
        eml_str = eml.build_eml_xml()
        print(eml_str)
        assert eml_str

        eml_node = metapype_io.from_xml(eml_str)
        assert isinstance(eml_node, Node)
        errs = list()
        validate.tree(eml_node, errs)
        assert len(errs) > 0

        eml_dataset_node = eml_node.find_single_node_by_path([names.DATASET])
        assert isinstance(eml_dataset_node, Node)

        eml_additional_metadata_node = eml_node.find_single_node_by_path([names.ADDITIONALMETADATA])
        assert isinstance(eml_additional_metadata_node, Node)

        with open("./input_files/eml//eml-sample.xml", "r") as f:
            xml = f.read().strip()
            expected_eml_node = metapype_io.from_xml(xml)
            expected_eml_dataset_node = expected_eml_node.find_single_node_by_path([names.DATASET])
            expected_eml_additional_metadata_node = expected_eml_node.find_single_node_by_path(
                [names.ADDITIONALMETADATA]
            )

        assert len(eml_node.children) == len(expected_eml_node.children)
        assert len(eml_dataset_node.children) == len(expected_eml_dataset_node.children)
        assert len(eml_additional_metadata_node.children) == len(expected_eml_additional_metadata_node.children)

        # Check 1st level of dataset
        dataset_node, expected_dataset_node = compare_node_contents(eml_node, expected_eml_node, [names.DATASET])

        # Check the creator and sub levels of individual name and address
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.CREATOR, names.INDIVIDUALNAME, names.ADDRESS],
        )

        # Check the contact and sub levels of individual name and address
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.CONTACT, names.INDIVIDUALNAME, names.ADDRESS],
        )

        # Check the keywords
        compare_node_contents(dataset_node, expected_dataset_node, [names.KEYWORDSET])

        # Check the coverage and sub level of geographic coverage and taxonomic coverage
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.COVERAGE, names.GEOGRAPHICCOVERAGE, names.TAXONOMICCOVERAGE],
        )

        # Check the additionalMetadata and sublevel citation
        metadata_node = eml_additional_metadata_node.find_child(names.METADATA)
        expected_metadata_node = expected_eml_additional_metadata_node.find_child(names.METADATA)
        compare_node_contents(
            metadata_node,
            expected_metadata_node,
            [names.CITATION, names.ADDITIONALINFO],
        )

    def test_build_eml(self, dataset, additional_metadata):
        eml = Eml()
        eml.dataset = dataset
        eml.additional_metadata = additional_metadata

        eml_str = eml.build_eml_xml()
        assert eml_str

        eml_node = metapype_io.from_xml(eml_str)
        assert isinstance(eml_node, Node)
        errs = list()
        validate.tree(eml_node, errs)
        assert len(errs) > 0

        eml_dataset_node = eml_node.find_single_node_by_path([names.DATASET])
        assert isinstance(eml_dataset_node, Node)

        eml_additional_metadata_node = eml_node.find_single_node_by_path([names.ADDITIONALMETADATA])
        assert isinstance(eml_additional_metadata_node, Node)

        with open("./input_files/eml//eml-sample.xml", "r") as f:
            xml = f.read().strip()
            expected_eml_node = metapype_io.from_xml(xml)
            expected_eml_dataset_node = expected_eml_node.find_single_node_by_path([names.DATASET])
            expected_eml_additional_metadata_node = expected_eml_node.find_single_node_by_path(
                [names.ADDITIONALMETADATA]
            )

        assert len(eml_node.children) == len(expected_eml_node.children)
        assert len(eml_dataset_node.children) == len(expected_eml_dataset_node.children)
        assert len(eml_additional_metadata_node.children) == len(expected_eml_additional_metadata_node.children)

        # Check 1st level of dataset
        dataset_node, expected_dataset_node = compare_node_contents(eml_node, expected_eml_node, [names.DATASET])

        # Check the creator and sub levels of individual name and address
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.CREATOR, names.INDIVIDUALNAME, names.ADDRESS],
        )

        # Check the contact and sub levels of individual name and address
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.CONTACT, names.INDIVIDUALNAME, names.ADDRESS],
        )

        # Check the keywords
        compare_node_contents(dataset_node, expected_dataset_node, [names.KEYWORDSET])

        # Check the coverage and sub level of geographic coverage and taxonomic coverage
        compare_node_contents(
            dataset_node,
            expected_dataset_node,
            [names.COVERAGE, names.GEOGRAPHICCOVERAGE, names.TAXONOMICCOVERAGE],
        )

        # Check the additionalMetadata and sublevel citation
        metadata_node = eml_additional_metadata_node.find_child(names.METADATA)
        expected_metadata_node = expected_eml_additional_metadata_node.find_child(names.METADATA)
        compare_node_contents(
            metadata_node,
            expected_metadata_node,
            [names.CITATION, names.ADDITIONALINFO],
        )

    def test_eml_only_dataset_supplied(self, dataset, caplog):
        eml = Eml(dataset=dataset)
        eml_str = eml.build_eml_xml()
        assert eml_str
        print(eml_str)
        assert EML_METADATA_NOT_COMPLETE not in caplog.messages

    def test_eml_dataset_incomplete(self, dataset, caplog):
        eml = Eml(dataset=Dataset())
        eml_str = eml.build_eml_xml()
        assert eml_str
        print(eml_str)
        assert EML_METADATA_NOT_COMPLETE in caplog.messages

    def test_eml_without_dataset(self, additional_metadata, caplog):
        eml = Eml(additional_metadata=additional_metadata)
        eml_str = eml.build_eml_xml()
        assert eml_str
        print(eml_str)
        assert EML_METADATA_NOT_COMPLETE in caplog.messages

    def test_eml_additional_metadata_incomplete(self, caplog):
        eml = Eml(additional_metadata=AdditionalMetadata())
        eml_str = eml.build_eml_xml()
        assert eml_str
        print(eml_str)
        assert EML_METADATA_NOT_COMPLETE in caplog.messages

    def test_eml_empty_dataset_and_metadata(self, caplog):
        eml = Eml(dataset=Dataset(), additional_metadata=AdditionalMetadata())
        eml_str = eml.build_eml_xml()
        assert eml_str
        print(eml_str)
        assert EML_METADATA_NOT_COMPLETE in caplog.messages
