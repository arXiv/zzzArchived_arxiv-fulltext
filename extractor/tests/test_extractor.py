"""E2E test for the extractor."""

from unittest import TestCase
import subprocess
import os
import tempfile
import shutil
import sys

DOCKER_REGISTRY = os.environ.get('DOCKER_REGISTRY', '')


class TestExtractorE2E(TestCase):
    """Ensure that the extractor can be built, and performs as expected."""

    __test__ = int(bool(os.environ.get('WITH_INTEGRATION', False)))

    @classmethod
    def setUpClass(cls):
        basepath, _ = os.path.split(os.path.abspath(__file__))
        runpath, _ = os.path.split(basepath)
        build_result = subprocess.run(
            "docker build %s -f %s/Dockerfile "
            "-t arxiv/fulltext-extractor:0.3" % (runpath, runpath),
            capture_output=True, shell=True
        )
        if build_result.returncode != 0:
          print(build_result.stdout.decode("utf-8"))
          print(build_result.stderr.decode("utf-8"), file=sys.stderr)
        assert build_result.returncode == 0

    def do_test_extract(self, pdf_filename):
        basepath, _ = os.path.split(os.path.abspath(__file__))
        pdf_path = tempfile.mkdtemp()
        shutil.copyfile(os.path.join(basepath, 'pdfs', pdf_filename),
                        os.path.join(pdf_path, pdf_filename))
        runpath, _ = os.path.split(basepath)

        extract_result = subprocess.run(
            "docker run -it -v %s:/pdfs "
            "arxiv/fulltext-extractor:0.3 /pdfs/%s" % (pdf_path, pdf_filename),
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
        return content

    def test_extract_1702_07336(self):
        content = self.do_test_extract('1702.07336.pdf')
        self.assertGreater(len(content), 50000)

    def test_extract_9912018(self):
        content = self.do_test_extract('9912018.pdf')
        self.assertGreater(len(content), 47000)

    def test_extract_1905_02187(self):
        content = self.do_test_extract('1905.02187.pdf')
        self.assertGreater(len(content), 43000)

    def test_extract_9108004(self):
        content = self.do_test_extract('9108004.pdf')
        self.assertGreater(len(content), 45000)

    def test_extract_9109009(self):
        content = self.do_test_extract('9109009.pdf')
        self.assertGreater(len(content), 27000)

    def test_extract_1905_00506(self):
        content = self.do_test_extract('1905.00506.pdf')
        self.assertGreater(len(content), 47000)

    def test_extract_1804_08269(self):
        content = self.do_test_extract('1804.08269.pdf')
        self.assertGreater(len(content), 47000)

    def test_extract_1512_03905(self):
        content = self.do_test_extract('1512.03905.pdf')
        self.assertGreater(len(content), 20000)
