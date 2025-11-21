from setuptools.command import install
import sys
import warnings
import subprocess
import tempfile
import os
import json
import shutil

CDKTF_CLI_VERSION = "0.20.9"
CDKTF_VERSION = "0.20.9"


class DFInstallCommand(install):
    """This class set the installation for the oneflow package"""

    def run(self):
        install.run(self)

        # Downloading the confluent-cloud into the python packages
        # check npm exists or not
        try:
            npm_cmd_result = subprocess.run(
                ["npm", "-v"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if npm_cmd_result.result_code == 0:
                try:
                    npm_cdktf_result = subprocess.run(
                        ["cdktf", "--version"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                except FileNotFoundError as e:
                    print("Installing the cdktf package....")
                    npm_cdktf_result = subprocess.run(
                        [
                            "npm",
                            "install",
                            "--global",
                            f"cdktf-cli@{CDKTF_CLI_VERSION}",
                            f"cdktf@{CDKTF_VERSION}",
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    if npm_cdktf_result.returncode != 0:
                        warnings.warn(
                            f"cdktf installation failed due {npm_cdktf_result.stderr.decode()}."
                        )
                        return
                    temp_dir = os.path.join(tempfile.gettempdir(), "confluent")
                    if not os.path.exists(temp_dir):
                        os.mkdir(temp_dir, mode=777)

                    confluent_cdktf_tml = {
                        "language": "python",
                        "app": "python3 ./main.py",
                        "sendCrashReports": "false",
                        "terraformProviders": ["confluentinc/confluent"],
                    }

                    with open(f"{temp_dir}/cdktf.json", "w") as f:
                        json.dump(confluent_cdktf_tml, f)
                    npm_confluent_provider_result = subprocess.run(
                        ["cdktf", "get", "-l", "python", "-o", f"{temp_dir}/download"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=temp_dir,
                    )
                    if npm_confluent_provider_result.returncode != 0:
                        warnings.warn(
                            f"confluent terraform provider installation failed due {npm_cdktf_result.stderr.decode()}."
                        )
                        return
                    setup_script = f"""
from setuptools import setup, find_packages

setup(
name="cdtf-confluent-provider",
version="0.1",
packages=find_packages(where="{temp_dir}/download", include=["confluent.*"]),
package_data={{
'': ['*'],  # Include all files in the directory
}},
include_package_data=True,
)
                        """
                    with open(os.path.join(f"{temp_dir}/download", "setup.py"), "w") as f:
                        f.write(setup_script)
                    pip_out = subprocess.run(
                        [sys.executable, "-m", "pip", "install", "."],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=f"{temp_dir}/download",
                    )
                    if pip_out.returncode != 0:
                        warnings.warn(
                            f"confluent terraform provider installation failed due {pip_out.stderr.decode()}."
                        )
                        return
                    shutil.rmtree(temp_dir,ignore_errors=True)

        except FileNotFoundError as e:
            warnings.warn(
                "node-js is not installed, Please install latest version of the node-js with npm package manager."
            )
        except Exception as e:
            warnings.warn(
                f"Error while installing the confluent terraform provider. Failed due to error {e}"
            )
