import sys
import csv
import base64
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton,
    QLineEdit, QFileDialog, QLabel, QMessageBox
)

from mcap.reader import make_reader
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message

# Get the LaserScan message type
LaserScan = get_message('sensor_msgs/msg/LaserScan')

def convert_mcap_to_csv(mcap_file, csv_file):
    """
    Convert all /scan LaserScan messages from the MCAP file to a CSV file.
    """
    try:
        with open(mcap_file, 'rb') as mcap_stream:
            reader = make_reader(mcap_stream)
            scan_messages = []

            # Iterate through messages
            # According to older MCAP versions, iter_messages() may return tuples: (schema, channel, message)
            # If your version returns tuples, unpack them. If it returns message objects, adjust accordingly.
            # We'll assume a tuple of (schema, channel, message) as per recent MCAP Python docs.
            # If this differs for your version, adjust accordingly.
            for schema, channel, message in reader.iter_messages():
                if channel.topic == "/scan":
                    # Deserialize the binary data into a LaserScan message
                    scan_msg = deserialize_message(message.data, LaserScan)

                    # Convert ranges and intensities to a semicolon-separated string
                    ranges_str = ";".join(str(r) for r in scan_msg.ranges)
                    intensities_str = ";".join(str(i) for i in scan_msg.intensities)

                    scan_messages.append({
                        "timestamp": message.log_time,
                        "angle_min": scan_msg.angle_min,
                        "angle_max": scan_msg.angle_max,
                        "angle_increment": scan_msg.angle_increment,
                        "time_increment": scan_msg.time_increment,
                        "scan_time": scan_msg.scan_time,
                        "range_min": scan_msg.range_min,
                        "range_max": scan_msg.range_max,
                        "ranges": ranges_str,
                        "intensities": intensities_str
                    })

            if not scan_messages:
                print("No /scan messages found in the MCAP file.")
                return False

            # Write to CSV
            fieldnames = list(scan_messages[0].keys())
            with open(csv_file, 'w', newline='') as out_file:
                writer = csv.DictWriter(out_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(scan_messages)

        print("Conversion complete!")
        return True
    except Exception as e:
        print(f"Error during conversion: {e}")
        return False


class MCAPConverterApp(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("MCAP to CSV Converter (LaserScan)")
        self.setGeometry(100, 100, 500, 200)

        layout = QVBoxLayout()

        # Label and input for MCAP file
        self.mcap_label = QLabel("Select MCAP File:")
        self.mcap_input = QLineEdit()
        self.mcap_browse_btn = QPushButton("Browse")
        self.mcap_browse_btn.clicked.connect(self.browse_mcap_file)

        layout.addWidget(self.mcap_label)
        layout.addWidget(self.mcap_input)
        layout.addWidget(self.mcap_browse_btn)

        # Label and input for CSV export location
        self.csv_label = QLabel("Select CSV Output File:")
        self.csv_input = QLineEdit()
        self.csv_browse_btn = QPushButton("Browse")
        self.csv_browse_btn.clicked.connect(self.browse_csv_file)

        layout.addWidget(self.csv_label)
        layout.addWidget(self.csv_input)
        layout.addWidget(self.csv_browse_btn)

        # Convert button
        self.convert_btn = QPushButton("Convert to CSV")
        self.convert_btn.clicked.connect(self.convert_to_csv)

        layout.addWidget(self.convert_btn)

        self.setLayout(layout)

    def browse_mcap_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select MCAP File", "", "MCAP Files (*.mcap)")
        if file_path:
            self.mcap_input.setText(file_path)

    def browse_csv_file(self):
        file_path, _ = QFileDialog.getSaveFileName(self, "Select CSV File", "", "CSV Files (*.csv)")
        if file_path:
            self.csv_input.setText(file_path)

    def convert_to_csv(self):
        mcap_file = self.mcap_input.text().strip()
        csv_file = self.csv_input.text().strip()

        if not mcap_file or not csv_file:
            QMessageBox.warning(self, "Input Error", "Please select both the MCAP file and the CSV export location.")
            return

        success = convert_mcap_to_csv(mcap_file, csv_file)
        if success:
            QMessageBox.information(self, "Success", "MCAP file successfully converted to CSV!")
        else:
            QMessageBox.critical(self, "Error", "An error occurred during the conversion.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MCAPConverterApp()
    window.show()
    sys.exit(app.exec())
