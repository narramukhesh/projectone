from setuptools.command.sdist import sdist
from setuptools import setup
from pathlib import Path
import sys
import warnings
import subprocess
import tempfile
import os
import json
import shutil

CDKTF_CLI_VERSION = "latest"
CDKTF_VERSION = "latest"


class DFInstallCommand(sdist):
    """This class set the installation for the oneflow package"""

    def run(self):

        if (
        os.getcwd() != str(Path(__file__).parent.absolute())
        ):
            os.chdir(Path(__file__).parent.absolute())

        temp_dir = os.path.join(tempfile.gettempdir(), "oneflow_confluent_provider")
        external_dir = f"{Path(__file__).parent.absolute()}/src/oneflow/core/external"

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir,ignore_errors=True)

        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir, mode=777)

        # Downloading the confluent-cloud into the python packages
        # check npm exists or not
        try:
            npm_cmd_result = subprocess.run(
                ["npm", "-v"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if npm_cmd_result.returncode == 0:
                try:
                    terraform_cmd_result = subprocess.run(
                ["terraform", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
                except FileNotFoundError as e:
                    raise OSError("\033[1mTerraform is not installed. Please follow https://developer.hashicorp.com/terraform/install according to your platform.\033[0m")

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
                        raise OSError(
                            f"\033[1m cdktf installation failed due {npm_cdktf_result.stderr.decode()}.\033[0m"
                        )
                        

                confluent_cdktf_tml = {
                        "language": "python",
                        "app": "python3 ./main.py",
                        "sendCrashReports": "false",
                        "terraformProviders": ["confluentinc/confluent"],
                    }

                with open(f"{temp_dir}/cdktf.json", "w") as f:
                    json.dump(confluent_cdktf_tml, f)
                print("\033[1m Generating the confluent terraform provider artifacts... \033[1m")
                npm_confluent_provider_result = subprocess.run(
                        ["cdktf", "get", "-l", "python", "-o", f"{temp_dir}/download"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=temp_dir,
                    )
                if npm_confluent_provider_result.returncode != 0:
                    raise OSError(
                            f"\033[1m confluent terraform provider installation failed due {npm_confluent_provider_result.stderr.decode()}.\033[0m"
                        )
                if os.path.exists(external_dir):
                    shutil.rmtree(external_dir,ignore_errors=True)
                os.makedirs(external_dir,mode=677,exist_ok=True)
                Path(f"{external_dir}/__init__.py").touch(mode=677,exist_ok=True)
                shutil.move(f"{temp_dir}/download",external_dir)
                
        except FileNotFoundError as e:
            raise OSError(
                "\033[1m npm is not installed, Please install latest version of the node-js should be greater than v22 with npm package manager. \033[0m"
            )
        except Exception as e:
            raise OSError(
                f"\033[1m Error while installing the confluent terraform provider. Failed due to error {e}\033[0m"
            )
        
        finally:
            shutil.rmtree(temp_dir,ignore_errors=True)
        
        sdist.run(self)


setup(
    cmdclass={'sdist':DFInstallCommand}
)