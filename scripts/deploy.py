#!/usr/bin/env python3
"""
Retail Insights Platform Deployment Script
Deploys the complete real-time retail analytics platform
"""

import sys
import subprocess
import argparse
import logging
import time
import json
from pathlib import Path
from typing import Dict, List, Optional
import requests
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

console = Console()

class RetailInsightsDeployer:
    """Deploys the Retail Insights Platform"""

    def __init__(self, config_path: str = "config/deployment.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self.project_root = Path(__file__).parent.parent

    def _load_config(self) -> Dict:
        """Load deployment configuration"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            console.print(f"[red]Configuration file not found: {self.config_path}[/red]")
            sys.exit(1)

    def _run_command(self, command: List[str], cwd: Optional[Path] = None) -> bool:
        """Run a shell command"""
        try:
            result = subprocess.run(
                command,
                cwd=cwd or self.project_root,
                capture_output=True,
                text=True,
                check=True
            )
            console.print(f"[green]✓ {command[0]} completed successfully[/green]")
            return True
        except subprocess.CalledProcessError as e:
            console.print(f"[red]✗ {command[0]} failed: {e.stderr}[/red]")
            return False

    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met"""
        console.print(Panel("Checking Prerequisites", style="blue"))

        prerequisites = [
            ("AWS CLI", ["aws", "--version"]),
            ("Terraform", ["terraform", "--version"]),
            ("Docker", ["docker", "--version"]),
            ("Python", ["python", "--version"]),
            ("Git", ["git", "--version"])
        ]

        all_good = True
        for name, command in prerequisites:
            if self._run_command(command):
                console.print(f"[green]✓ {name} is installed[/green]")
            else:
                console.print(f"[red]✗ {name} is not installed or not in PATH[/red]")
                all_good = False

        return all_good

    def setup_aws_credentials(self) -> bool:
        """Setup AWS credentials"""
        console.print(Panel("Setting up AWS Credentials", style="blue"))

        # Check if AWS credentials are configured
        if self._run_command(["aws", "sts", "get-caller-identity"]):
            console.print("[green]✓ AWS credentials are configured[/green]")
            return True

        # Prompt for AWS credentials
        console.print("[yellow]AWS credentials not found. Please configure them:[/yellow]")

        access_key = Prompt.ask("AWS Access Key ID")
        secret_key = Prompt.ask("AWS Secret Access Key", password=True)
        region = Prompt.ask("AWS Region", default="us-east-1")

        # Configure AWS CLI
        commands = [
            ["aws", "configure", "set", "aws_access_key_id", access_key],
            ["aws", "configure", "set", "aws_secret_access_key", secret_key],
            ["aws", "configure", "set", "default.region", region]
        ]

        for command in commands:
            if not self._run_command(command):
                return False

        console.print("[green]✓ AWS credentials configured successfully[/green]")
        return True

    def deploy_infrastructure(self) -> bool:
        """Deploy AWS infrastructure using Terraform"""
        console.print(Panel("Deploying AWS Infrastructure", style="blue"))

        terraform_dir = self.project_root / "terraform"

        # Initialize Terraform
        if not self._run_command(["terraform", "init"], cwd=terraform_dir):
            return False

        # Plan Terraform deployment
        if not self._run_command(["terraform", "plan", "-out=tfplan"], cwd=terraform_dir):
            return False

        # Confirm deployment
        if not Confirm.ask("Do you want to proceed with the infrastructure deployment?"):
            console.print("[yellow]Infrastructure deployment cancelled[/yellow]")
            return False

        # Apply Terraform plan
        if not self._run_command(["terraform", "apply", "tfplan"], cwd=terraform_dir):
            return False

        # Get outputs
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=terraform_dir,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            outputs = json.loads(result.stdout)
            self._save_outputs(outputs)
            console.print("[green]✓ Infrastructure deployed successfully[/green]")
            return True

        return False

    def _save_outputs(self, outputs: Dict):
        """Save Terraform outputs to file"""
        outputs_file = self.project_root / "config" / "terraform_outputs.json"
        outputs_file.parent.mkdir(exist_ok=True)

        with open(outputs_file, 'w') as f:
            json.dump(outputs, f, indent=2)

        console.print(f"[green]✓ Terraform outputs saved to {outputs_file}[/green]")

    def deploy_kafka_cluster(self) -> bool:
        """Deploy Kafka cluster"""
        console.print(Panel("Deploying Kafka Cluster", style="blue"))

        kafka_dir = self.project_root / "kafka"

        # Deploy Kafka using Docker Compose
        if not self._run_command(["docker-compose", "up", "-d"], cwd=kafka_dir):
            return False

        # Wait for Kafka to be ready
        console.print("[yellow]Waiting for Kafka cluster to be ready...[/yellow]")
        time.sleep(30)

        # Check Kafka health
        if self._check_kafka_health():
            console.print("[green]✓ Kafka cluster deployed successfully[/green]")
            return True

        return False

    def _check_kafka_health(self) -> bool:
        """Check Kafka cluster health"""
        try:
            # Simple health check - try to connect to Kafka
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            producer.close()
            return True
        except Exception as e:
            console.print(f"[red]Kafka health check failed: {e}[/red]")
            return False

    def setup_databricks(self) -> bool:
        """Setup Databricks workspace"""
        console.print(Panel("Setting up Databricks Workspace", style="blue"))

        # Check if Databricks CLI is installed
        if not self._run_command(["databricks", "--version"]):
            console.print("[yellow]Databricks CLI not found. Please install it first.[/yellow]")
            return False

        # Configure Databricks
        host = Prompt.ask("Databricks Workspace URL")
        token = Prompt.ask("Databricks Access Token", password=True)

        commands = [
            ["databricks", "configure", "--token", "--host", host],
            ["databricks", "workspace", "mkdir", "/Shared/RetailInsights"],
            ["databricks", "fs", "mkdir", "dbfs:/retail-insights"]
        ]

        for command in commands:
            if not self._run_command(command):
                return False

        # Upload notebooks
        notebooks_dir = self.project_root / "databricks" / "notebooks"
        if notebooks_dir.exists():
            for notebook in notebooks_dir.glob("*.py"):
                if not self._run_command([
                    "databricks", "workspace", "import",
                    str(notebook),
                    f"/Shared/RetailInsights/{notebook.stem}"
                ]):
                    return False

        console.print("[green]✓ Databricks workspace configured successfully[/green]")
        return True

    def deploy_airflow(self) -> bool:
        """Deploy Apache Airflow"""
        console.print(Panel("Deploying Apache Airflow", style="blue"))

        airflow_dir = self.project_root / "airflow"

        # Deploy Airflow using Docker Compose
        if not self._run_command(["docker-compose", "up", "-d"], cwd=airflow_dir):
            return False

        # Wait for Airflow to be ready
        console.print("[yellow]Waiting for Airflow to be ready...[/yellow]")
        time.sleep(60)

        # Check Airflow health
        if self._check_airflow_health():
            console.print("[green]✓ Airflow deployed successfully[/green]")
            return True

        return False

    def _check_airflow_health(self) -> bool:
        """Check Airflow health"""
        try:
            response = requests.get("http://localhost:8080/health", timeout=30)
            return response.status_code == 200
        except Exception as e:
            console.print(f"[red]Airflow health check failed: {e}[/red]")
            return False

    def setup_monitoring(self) -> bool:
        """Setup monitoring with Prometheus and Grafana"""
        console.print(Panel("Setting up Monitoring", style="blue"))

        monitoring_dir = self.project_root / "monitoring"

        # Deploy monitoring stack
        if not self._run_command(["docker-compose", "up", "-d"], cwd=monitoring_dir):
            return False

        # Wait for services to be ready
        console.print("[yellow]Waiting for monitoring services to be ready...[/yellow]")
        time.sleep(30)

        # Check monitoring health
        if self._check_monitoring_health():
            console.print("[green]✓ Monitoring setup completed successfully[/green]")
            return True

        return False

    def _check_monitoring_health(self) -> bool:
        """Check monitoring services health"""
        try:
            # Check Prometheus
            prometheus_response = requests.get("http://localhost:9090/-/healthy", timeout=10)
            # Check Grafana
            grafana_response = requests.get("http://localhost:3000/api/health", timeout=10)

            return prometheus_response.status_code == 200 and grafana_response.status_code == 200
        except Exception as e:
            console.print(f"[red]Monitoring health check failed: {e}[/red]")
            return False

    def start_data_pipeline(self) -> bool:
        """Start the data pipeline"""
        console.print(Panel("Starting Data Pipeline", style="blue"))

        # Start Kafka producer
        producer_script = self.project_root / "kafka" / "producers" / "clickstream_producer.py"
        if producer_script.exists():
            console.print("[yellow]Starting clickstream data producer...[/yellow]")
            subprocess.Popen([
                "python", str(producer_script),
                "--events-per-second", "10"
            ])

        # Start Kafka consumer
        consumer_script = self.project_root / "kafka" / "consumers" / "databricks_consumer.py"
        if consumer_script.exists():
            console.print("[yellow]Starting Databricks consumer...[/yellow]")
            subprocess.Popen([
                "python", str(consumer_script),
                "--databricks-host", self.config.get("databricks_host", ""),
                "--databricks-token", self.config.get("databricks_token", "")
            ])

        console.print("[green]✓ Data pipeline started successfully[/green]")
        return True

    def run_tests(self) -> bool:
        """Run system tests"""
        console.print(Panel("Running System Tests", style="blue"))

        tests_dir = self.project_root / "tests"

        if tests_dir.exists():
            if self._run_command(["python", "-m", "pytest", "tests/", "-v"]):
                console.print("[green]✓ All tests passed[/green]")
                return True
            else:
                console.print("[red]✗ Some tests failed[/red]")
                return False

        console.print("[yellow]No tests found[/yellow]")
        return True

    def generate_summary(self):
        """Generate deployment summary"""
        console.print(Panel("Deployment Summary", style="green"))

        summary_table = Table(title="Service Status")
        summary_table.add_column("Service", style="cyan")
        summary_table.add_column("Status", style="green")
        summary_table.add_column("URL", style="blue")

        services = [
            ("Kafka", "Running", "localhost:9092"),
            ("Airflow", "Running", "http://localhost:8080"),
            ("Prometheus", "Running", "http://localhost:9090"),
            ("Grafana", "Running", "http://localhost:3000"),
            ("Databricks", "Configured", self.config.get("databricks_host", "N/A")),
            ("Redshift", "Deployed", "Via AWS Console")
        ]

        for service, status, url in services:
            summary_table.add_row(service, status, url)

        console.print(summary_table)

        console.print("\n[bold green]🎉 Retail Insights Platform deployed successfully![/bold green]")
        console.print("\n[bold]Next Steps:[/bold]")
        console.print("1. Access Grafana dashboard: http://localhost:3000")
        console.print("2. Access Airflow UI: http://localhost:8080")
        console.print("3. Monitor data pipeline in real-time")
        console.print("4. Connect Power BI to Redshift for visualization")

    def deploy(self, skip_tests: bool = False):
        """Deploy the complete platform"""
        console.print(Panel.fit(
            "[bold blue]Retail Insights Platform Deployment[/bold blue]\n"
            "Real-time retail analytics platform with Kafka, Databricks, Airflow, and Redshift",
            style="blue"
        ))

        # Check prerequisites
        if not self.check_prerequisites():
            console.print("[red]Prerequisites check failed. Please install required tools.[/red]")
            return False

        # Setup AWS credentials
        if not self.setup_aws_credentials():
            console.print("[red]AWS credentials setup failed.[/red]")
            return False

        # Deploy infrastructure
        if not self.deploy_infrastructure():
            console.print("[red]Infrastructure deployment failed.[/red]")
            return False

        # Deploy Kafka
        if not self.deploy_kafka_cluster():
            console.print("[red]Kafka deployment failed.[/red]")
            return False

        # Setup Databricks
        if not self.setup_databricks():
            console.print("[red]Databricks setup failed.[/red]")
            return False

        # Deploy Airflow
        if not self.deploy_airflow():
            console.print("[red]Airflow deployment failed.[/red]")
            return False

        # Setup monitoring
        if not self.setup_monitoring():
            console.print("[red]Monitoring setup failed.[/red]")
            return False

        # Start data pipeline
        if not self.start_data_pipeline():
            console.print("[red]Data pipeline startup failed.[/red]")
            return False

        # Run tests
        if not skip_tests and not self.run_tests():
            console.print("[red]System tests failed.[/red]")
            return False

        # Generate summary
        self.generate_summary()

        return True

def main():
    parser = argparse.ArgumentParser(description="Deploy Retail Insights Platform")
    parser.add_argument("--config", default="config/deployment.json", help="Configuration file path")
    parser.add_argument("--skip-tests", action="store_true", help="Skip running tests")

    args = parser.parse_args()

    deployer = RetailInsightsDeployer(args.config)

    try:
        success = deployer.deploy(skip_tests=args.skip_tests)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Deployment interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Deployment failed: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
