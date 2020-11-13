"""Tests using NLP."""
import os
import glob
import subprocess
import unittest
from subprocess import CalledProcessError
from typing import Dict, List, Tuple
from unittest import TestCase


def get_mypy_failures(mypy_call: List[str]) -> Tuple[str, int]:
    test_failures: int = 0
    test_result: str = ""
    try:
        test_result = subprocess.check_output(
            mypy_call, env=os.environ).decode()
    except CalledProcessError as ex:
        test_result = ex.output.decode()
    test_failures += len(list(
      filter(lambda line: not line.startswith("Success")
      , test_result.splitlines())))
    return (test_result, test_failures)

class MyPyTest(TestCase):
    """Class for testing modules with mypy."""

    def test_run_mypy_module(self) -> None:
        """Run mypy on all module sources."""
        mypy_call: List[str] = ["mypy"] + self.mypy_opts + ["-p", self.pkgname]
        (result, failures) = get_mypy_failures(mypy_call)
        self.assertEqual(failures, 0, f'mypy on {self.pkgname}:\n{result}')

    def test_run_mypy_tests(self) -> None:
        """Run mypy on all tests in module under the tests directory."""
        test_out_file_name = "mypy_test_errors.txt"
        if os.path.exists(test_out_file_name):
            os.remove(test_out_file_name)
        test_out_file = open(test_out_file_name, "w+")
        for test_file in glob.iglob(f'{os.getcwd()}/tests/**/*.py',
                                    recursive=True):
            mypy_call: List[str] = ["mypy"] + self.mypy_opts + [test_file]
            (test_result, test_failures) = get_mypy_failures(mypy_call)
            test_out_file.writelines(test_result)
        test_out_file.close()
        # Choose arbitrary number of test errors to tolerate:
        # TODO: probably need to bring this down slightly
        print('***** * * *   Current number of mypy errors in tests:'
              f'{test_failures}  * * * *****\n'
              f'***** * * *   See {test_out_file_name} for error log  * * * *****'
              )

    def __init__(self, *args: str, **kwargs: str) -> None:
        """Set up some common variables."""
        super().__init__(*args, **kwargs)
        self.pkgname: str = "fulltext"
        my_env = os.environ.copy()
        self.pypath: str = my_env.get("PYTHONPATH", os.getcwd())
        # should now use mypy.ini for mypy options
        self.mypy_opts: List[str] = []


if __name__ == '__main__':
    unittest.main()
