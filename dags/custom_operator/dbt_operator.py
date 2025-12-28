from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator
from dbt.cli.main import dbtRunner, dbtRunnerResult

class DbtCoreOperator(BaseOperator):
    def __init__(
        self,
        dbt_project_dir: str,
        dbt_profiles_dir: str,
        dbt_command: str,
        target: str = None,
        select:str = None,
        dbt_vars: dict = None,
        full_refresh: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.target = target
        self.select = select
        self.runner = dbtRunner() 
        self.dbt_vars = dbt_vars or {}
        self.full_refresh = full_refresh

    def execute(self, context):
        command_args = [
            self.dbt_command,
            "--profiles-dir", self.dbt_profiles_dir,
            "--project-dir", self.dbt_project_dir
        ]
        if self.target:
            command_args.extend(["--target", self.target])
            
        if self.select:
            command_args.extend(["--select", self.select])

        if self.full_refresh:
            command_args.extend(["--full-refresh"])
        
        if self.dbt_vars:
            vars_string = " ".join([f"{k}: {v}" for k, v in self.dbt_vars.items()])
            command_args.extend(["--vars", f"'{vars_string}'"])

        self.log.info("Executing dbt command: %s", " ".join(command_args))

        try:
            res: dbtRunnerResult = self.runner.invoke(command_args)
        except Exception as exc:
            raise AirflowException(f"dbt {self.dbt_command} command failed") from exc

        res_success = getattr(res, "success", False)
        res_exception = getattr(res, "exception", None)
        res_result = getattr(res, "result", None)

        if not res_success:
            error_message = f"dbt {self.dbt_command} command failed"
            if res_exception:
                error_message = f"{error_message}: {res_exception}"
            self.log.error(error_message)
            raise AirflowException(error_message)

        if res_result:
            for r in res_result:
                self.log.info("%s: %s", r.node.name, r.status)
        else:
            self.log.info("dbt command completed without a result set.")

        self.log.info("dbt command executed successfully.")
