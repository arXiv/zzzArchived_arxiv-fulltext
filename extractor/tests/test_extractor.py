"""E2E test for the extractor."""

from unittest import TestCase
import subprocess
import os
import tempfile
import shutil

DOCKER_REGISTRY = os.environ.get('DOCKER_REGISTRY', '')


class TestExtractorE2E(TestCase):
    """Ensure that the extractor can be built, and performs as expected."""

    def test_extract_fulltext(self):
        pdf_filename = '1702.07336.pdf'
        basepath, _ = os.path.split(os.path.abspath(__file__))
        pdf_path = tempfile.mkdtemp()
        shutil.copyfile(os.path.join(basepath, 'pdfs', pdf_filename),
                        os.path.join(pdf_path, pdf_filename))
        runpath, _ = os.path.split(basepath)

        build_result = subprocess.run(
            "docker build %s -f %s/Dockerfile -t arxiv/fulltext" % (runpath, runpath),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        self.assertEqual(
            build_result.returncode,
            0,
            "%s\n\n%s" % (build_result.stdout, build_result.stderr)
        )

        extract_result = subprocess.run(
            "docker run -it -v %s:/pdfs arxiv/fulltext /scripts/extract.sh /pdfs/%s" % (pdf_path, pdf_filename),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        self.assertEqual(
            extract_result.returncode,
            0,
            "%s\n\n%s" % (extract_result.stdout, extract_result.stderr)
        )

        txt_path = os.path.join(pdf_path, pdf_filename.replace('.pdf', '.txt'))
        self.assertTrue(os.path.exists(txt_path))
        try:
            with open(txt_path, encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            self.fail('Failed to open output file: %s' % e)

        self.assertGreater(len(content), 50000)
