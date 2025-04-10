import pandas as pd
import dwcahandler
from dwcahandler.dwca import ContentData, CoreOrExtType, MetaElementTypes
from dwcahandler.dwca.core_dwca import Dwca
from operator import attrgetter
import logging
import pytest

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("test_multimedia_content")

MIMETYPE_IMAGE_URL = "https://www.gstatic.com/webp/gallery/1.webp"
INVALID_MIMETYPE_URL = "https://invalid.url.jpeg"
IMAGE_URL = "https://images.ala.org.au/image/proxyImageThumbnailLarge?imageId=a36b5634-0277-47c7-b4e3-383e24ce8d1a"
AUDIO_URL = "https://images.ala.org.au/image/proxyImage?imageId=480f5f5e-e96c-4ae3-8230-c53a37bc542e"
VIDEO_URL = "https://images.ala.org.au/image/proxyImage?imageId=537799d7-f4d6-490c-a24c-6a94bfd5e857"
INVALID_URL = "test"
DELETED_MEDIA_URL = "https://images.ala.org.au/image/proxyImageThumbnailLarge?imageId=nonexistent"

image_ext = ContentData(data=pd.DataFrame(data=[["1", IMAGE_URL],
                                                ["2", AUDIO_URL],
                                                ["3", VIDEO_URL],
                                                ["3", MIMETYPE_IMAGE_URL]],
                                          columns=['occurrenceID', 'identifier']),
                        type=MetaElementTypes.MULTIMEDIA,
                        keys=['occurrenceID'])


def mock_guess_type(url):
    if url == MIMETYPE_IMAGE_URL:
        return ('image/webp', None)
    elif url == INVALID_MIMETYPE_URL:
        return ('image/jpeg', None)
    return (None, None)


@pytest.fixture
def mock_mime_types(monkeypatch, request):
    if request.config.getoption("--github-action-run"):
        monkeypatch.setattr(dwcahandler.dwca.core_dwca.mimetypes, "guess_type", mock_guess_type)


class TestMultimediaExtension:

    def test_extract_associate_media(self):
        """
        Test for associated media to be expanded into multimedia extension
        """
        occ_associated_media_df = pd.DataFrame(data=[["1", "species1", IMAGE_URL],
                                                     ["2", "species2", AUDIO_URL],
                                                     ["3", "species3", f"{VIDEO_URL}|{MIMETYPE_IMAGE_URL}"]],
                                               columns=['occurrenceID', 'scientificName', 'associatedMedia'])

        dwca = Dwca()

        dwca.extract_csv_content(csv_info=ContentData(data=occ_associated_media_df,
                                                      keys=['occurrenceID'],
                                                      type=MetaElementTypes.OCCURRENCE),
                                 core_ext_type=CoreOrExtType.CORE)

        associated_media_image_ext = dwca.convert_associated_media_to_extension()

        assert 'associatedMedia' not in dwca.core_content.df_content.columns
        assert sorted(list(map(attrgetter('field_name'), dwca.meta_content.meta_elements[0].fields))) == \
               sorted(['occurrenceID', 'scientificName'])

        pd.testing.assert_frame_equal(associated_media_image_ext.data.reset_index(drop=True), image_ext.data)
        assert associated_media_image_ext.type == image_ext.type
        assert associated_media_image_ext.keys[0] == image_ext.keys[0]

        dwca.extract_csv_content(csv_info=associated_media_image_ext,
                                 core_ext_type=CoreOrExtType.EXTENSION)

        # Compare multimedia ext dataframe (without the coreid) against the expected image_ext dataframe
        pd.testing.assert_frame_equal(dwca.ext_content[0].df_content.reset_index(drop=True),
                                      image_ext.data, check_index_type=False)

        # Check the meta content is updated
        assert sorted(list(map(attrgetter('field_name'), dwca.meta_content.meta_elements[1].fields))) == \
               sorted(["identifier", "occurrenceID"])

        assert dwca.meta_content.meta_elements[1].core_id.index == dwca.meta_content.meta_elements[1].fields[0].index

    def test_fill_additional_multimedia_info(self, mock_mime_types):
        """
        Test for fill additional multimedia info if format and type is not provided
        :return:
        """
        dwca = Dwca()

        # Extract core occurrence
        dwca.extract_csv_content(csv_info=ContentData(data=pd.DataFrame(data=[["1", "species1"],
                                                                              ["2", "species2"],
                                                                              ["3", "species3"]],
                                                                        columns=['occurrenceID', 'scientificName']),
                                                      type=MetaElementTypes.OCCURRENCE,
                                                      keys=['occurrenceID']),
                                 core_ext_type=CoreOrExtType.CORE)

        # Extract multimedia ext without format
        dwca.extract_csv_content(csv_info=image_ext, core_ext_type=CoreOrExtType.EXTENSION)

        # Fill multimedia info
        dwca.fill_additional_info()

        expected_multimedia_df = pd.DataFrame(data=[["1", IMAGE_URL, None, None],
                                                    ["2", AUDIO_URL, None, None],
                                                    ["3", VIDEO_URL, None, None],
                                                    ["3", MIMETYPE_IMAGE_URL, 'image/webp', 'StillImage']],
                                              columns=['occurrenceID', 'identifier', 'format', 'type'])

        # Test that the multimedia extension will now contain the format and type
        pd.testing.assert_frame_equal(dwca.ext_content[0].df_content, expected_multimedia_df)

    def test_fill_multimedia_info_with_format_type_partially_supplied(self, mock_mime_types):
        """
        Test fill_additional_multimedia_info if format or type is already present.
        Calling fill_additional_multimedia_info should not change the existing values in the content
        if it is provided but if format and type is not provided, fill additional info should try
        to populate the value
        """
        dwca = Dwca()

        # Extract core occurrence
        dwca.extract_csv_content(csv_info=ContentData(data=pd.DataFrame(data=[["1", "species1"],
                                                                              ["2", "species2"],
                                                                              ["3", "species3"],
                                                                              ["4", "species4"],
                                                                              ["5", "species5"],
                                                                              ["6", "species6"],
                                                                              ["7", "species7"],
                                                                              ["8", "species8"],
                                                                              ["9", "species9"]],
                                                                        columns=['occurrenceID', 'scientificName']),
                                                      type=MetaElementTypes.OCCURRENCE,
                                                      keys=['occurrenceID']),
                                 core_ext_type=CoreOrExtType.CORE)

        image_data = [["1", IMAGE_URL, "image/webp", "StillImage"],
                      ["2", AUDIO_URL, "audio/mp3", None],
                      ["3", VIDEO_URL, None, "MovingImage"],
                      ["4", INVALID_URL, None, None],
                      ["5", INVALID_URL, 'invalidformat', None],
                      ["6", INVALID_URL, 'image/jpeg', None],
                      ["7", DELETED_MEDIA_URL, None, None],
                      ["8", INVALID_MIMETYPE_URL, None, None],
                      ["9", None, None, None]]

        # Extract multimedia ext without format
        dwca.extract_csv_content(csv_info=ContentData(data=pd.DataFrame(data=image_data,
                                                                        columns=["occurrenceID", "identifier",
                                                                                 "format", "type"]),
                                                      type=MetaElementTypes.MULTIMEDIA),
                                 core_ext_type=CoreOrExtType.EXTENSION)

        # Fill multimedia extension info
        dwca.fill_additional_info()

        expected_image_data = [["1", IMAGE_URL, "image/webp", "StillImage"],
                               ["2", AUDIO_URL, "audio/mp3", "Sound"],
                               ["3", VIDEO_URL, None, "MovingImage"],
                               ["4", INVALID_URL, None, None],
                               ["5", INVALID_URL, 'invalidformat', None],
                               ["6", INVALID_URL, 'image/jpeg', 'StillImage'],
                               ["7", DELETED_MEDIA_URL, None, None],
                               ["8", INVALID_MIMETYPE_URL, 'image/jpeg', 'StillImage'],
                               ["9", None, None, None]]

        expected_multimedia_df = pd.DataFrame(data=expected_image_data,
                                              columns=['occurrenceID', 'identifier', 'format', 'type'])

        # Test that the multimedia extension format and type is filled if none provided but
        # if format and type is provided it remains as provided
        pd.testing.assert_frame_equal(dwca.ext_content[0].df_content, expected_multimedia_df)

    def test_fill_multimedia_info_type_from_format(self, mock_mime_types):
        """
        Test fill_additional_multimedia_info if only format is already present.
        Calling fill_additional_multimedia_info should not change the existing values in the content
        """
        dwca = Dwca()

        # Extract core occurrence
        dwca.extract_csv_content(csv_info=ContentData(data=pd.DataFrame(data=[["1", "species1"],
                                                                              ["2", "species2"],
                                                                              ["3", "species3"],
                                                                              ["4", "species4"],
                                                                              ["5", "species5"],
                                                                              ["6", "species6"],
                                                                              ["7", "species7"],
                                                                              ["8", "species8"]],
                                                                        columns=['occurrenceID', 'scientificName']),
                                                      type=MetaElementTypes.OCCURRENCE,
                                                      keys=['occurrenceID']),
                                 core_ext_type=CoreOrExtType.CORE)

        image_data = [["1", IMAGE_URL, "image/webp"],
                      ["2", AUDIO_URL, "audio/mp3"],
                      ["3", VIDEO_URL, "video/mp4"],
                      ["4", INVALID_URL, None],
                      ["5", INVALID_URL, 'invalidformat'],
                      ["6", MIMETYPE_IMAGE_URL, None],
                      ["7", DELETED_MEDIA_URL, None],
                      ["8", INVALID_MIMETYPE_URL, None]]

        # Extract multimedia ext without format
        dwca.extract_csv_content(csv_info=ContentData(data=pd.DataFrame(data=image_data,
                                                                        columns=["occurrenceID", "identifier",
                                                                                 "format"]),
                                                      type=MetaElementTypes.MULTIMEDIA),
                                 core_ext_type=CoreOrExtType.EXTENSION)

        # Fill multimedia extension info
        dwca.fill_additional_info()

        expected_image_data = [["1", IMAGE_URL, "image/webp", "StillImage"],
                               ["2", AUDIO_URL, "audio/mp3", "Sound"],
                               ["3", VIDEO_URL, "video/mp4", "MovingImage"],
                               ["4", INVALID_URL, None, None],
                               ["5", INVALID_URL, 'invalidformat', None],
                               ["6", MIMETYPE_IMAGE_URL, 'image/webp', 'StillImage'],
                               ["7", DELETED_MEDIA_URL, None, None],
                               ["8", INVALID_MIMETYPE_URL, 'image/jpeg', 'StillImage']]

        expected_multimedia_df = pd.DataFrame(data=expected_image_data,
                                              columns=['occurrenceID', 'identifier', 'format', 'type'])

        # Test that the multimedia extension format and type is filled if none provided but
        # if format and type is provided it remains as provided
        pd.testing.assert_frame_equal(dwca.ext_content[0].df_content, expected_multimedia_df)
