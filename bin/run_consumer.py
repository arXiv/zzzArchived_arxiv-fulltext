"""Execute the KCL consumer process."""

from amazon_kclpy import kcl
from fulltext.agent.consumer import RecordProcessor


if __name__ == "__main__":
    kcl_process = kcl.KCLProcess(RecordProcessor())
    kcl_process.run()
