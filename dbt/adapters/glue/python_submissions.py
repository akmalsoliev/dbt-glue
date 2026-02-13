import ast
import time
import uuid
from typing import Any, Dict, List

from dbt.adapters.base import PythonJobHelper
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.glue import GlueCredentials


class GluePythonJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: GlueCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model

        # Extract packages from model config (dbt Core standard)
        self.packages = parsed_model.get("config", {}).get("packages", []) or []

        self.timeout = self.parsed_model["config"].get(
            "timeout", 60 * 60
        )  # Default 1 hour
        self.polling_interval = 10

    @staticmethod
    def _extract_packages_from_code(compiled_code: str) -> List[str]:
        """Extract packages from dbt.config(packages=[...]) in compiled code.

        Fallback for when dbt-core does not pass packages through
        parsed_model["config"].
        """
        try:
            tree = ast.parse(compiled_code)
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "config"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "dbt"
                ):
                    for kw in node.keywords:
                        if kw.arg == "packages" and isinstance(kw.value, ast.List):
                            return [
                                elt.value
                                for elt in kw.value.elts
                                if isinstance(elt, ast.Constant)
                            ]
        except Exception:
            pass
        return []

    def submit(self, compiled_code: str) -> None:
        """Submit Python code to existing Glue session for execution"""
        # Import here to avoid circular imports
        from dbt.adapters.glue.gluedbapi.connection import GlueConnection

        # Create connection using existing logic (reuses session creation)
        connection = GlueConnection(self.credentials)

        # Establish connection by getting cursor (this triggers session creation/reuse)
        cursor = connection.cursor()

        # Get the Glue client and session ID from the existing connection
        glue_client = connection.client
        session_id = connection.session_id

        print(f"DEBUG: Using Glue session: {session_id}")

        # Merge packages from parsed_model config and dbt.config() in compiled code
        code_packages = self._extract_packages_from_code(compiled_code)
        packages = list(set(dict.fromkeys(self.packages + code_packages)))

        try:
            # Install model-level packages before running the model code
            if packages:
                pkg_list = ", ".join(f'"{pkg}"' for pkg in packages)
                install_code = (
                    "import subprocess, sys\n"
                    f"subprocess.check_call([sys.executable, '-m', 'pip', 'install', {pkg_list}, '-q'])"
                )
                print(f"DEBUG: Installing packages: {packages}")
                install_statement_id = self._run_statement(
                    glue_client, session_id, install_code
                )
                self._wait_for_statement_completion(
                    glue_client, session_id, install_statement_id
                )

            # Run the actual Python code
            statement_id = self._run_statement(glue_client, session_id, compiled_code)

            # Wait for completion
            self._wait_for_statement_completion(glue_client, session_id, statement_id)

        except Exception as e:
            raise DbtRuntimeError(f"Python model execution failed: {str(e)}")
        finally:
            connection.close()

    def _run_statement(self, glue_client, session_id, code):
        """Run a Python statement in the existing Glue session"""
        response = glue_client.run_statement(SessionId=session_id, Code=code)
        return response["Id"]

    def _wait_for_statement_completion(self, glue_client, session_id, statement_id):
        """Wait for a statement to complete execution"""
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            response = glue_client.get_statement(SessionId=session_id, Id=statement_id)
            state = response["Statement"]["State"]

            if state == "AVAILABLE":
                # Check for errors
                output = response["Statement"].get("Output", {})
                status = output.get("Status", "")
                if status.lower() == "error":
                    error_message = output.get("ErrorName", "")
                    error_value = output.get("ErrorValue", "")
                    traceback = output.get("Traceback", "")

                    # Print the full output for debugging
                    print(f"DEBUG: Statement output: {output}")

                    raise DbtRuntimeError(
                        f"Python model failed with error: {error_message}\n{error_value}\n{traceback}"
                    )

                # Print the output for debugging
                print(f"DEBUG: Statement completed successfully. Output: {output}")
                return
            elif state in ("CANCELLED", "ERROR"):
                raise DbtRuntimeError(f"Statement execution failed with state: {state}")

            time.sleep(self.polling_interval)

        raise DbtRuntimeError("Timed out waiting for statement to complete")
