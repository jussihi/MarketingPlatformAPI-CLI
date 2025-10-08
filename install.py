import os
import sys


scriptdir = os.path.dirname(os.path.abspath(sys.argv[0]))

os.system(f"python3 -m venv {scriptdir}/.venv")
print(f"Created venv to to {scriptdir}/.venv")

os.system(
        f"{scriptdir}/.venv/bin/pip install -r " + f"{scriptdir}/requirements.txt"
    )
print("Installed requirements")
print("To activate the venv, run:")
print(f"source {scriptdir}/.venv/bin/activate")
print("if you are on Windows (CMD), run:")
print(f"{scriptdir}/.venv/Scripts/activate")
print("if you are on Windows (Powershell), run:")
print(f"{scriptdir}/.venv/Scripts/Activate.ps1")