from datetime import datetime
import json
from colorama import Fore, Style

class AlertHandler:
    def __init__(self, webhook_url=None):
        self.webhook_url = webhook_url

    def send_alert(self, report, level='INFO'):
        """
        Simulates sending an alert (e.g., to Slack/Email).
        For this project, we print to console with colors and log to a file.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        alert_msg = f"\n[{timestamp}] DATA QUALITY ALERT - Level: {level}\n"
        alert_msg += "="*50 + "\n"
        alert_msg += f"Dataset: {report.get('dataset_name', 'Unknown')}\n"
        alert_msg += f"Total Records: {report.get('total_records', 0)}\n"
        alert_msg += f"Passed Checks: {report.get('passed_checks', 0)}\n"
        alert_msg += f"Failed Checks: {report.get('failed_checks', 0)}\n"
        
        if report.get('failures'):
            alert_msg += "\nFAILURE DETAILS:\n"
            for failure in report['failures']:
                alert_msg += f"- {failure['check']}: {failure['message']}\n"
        
        alert_msg += "="*50 + "\n"

        # Print to console
        if level == 'CRITICAL':
            print(Fore.RED + alert_msg + Style.RESET_ALL)
        elif level == 'WARNING':
            print(Fore.YELLOW + alert_msg + Style.RESET_ALL)
        else:
            print(Fore.GREEN + alert_msg + Style.RESET_ALL)

        # Log to file
        with open('dq_alerts.log', 'a') as f:
            f.write(alert_msg + "\n")
            
        return True
