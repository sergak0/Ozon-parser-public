apt install python3-pip -y
sudo apt-get install python3-tk -y
apt install python3.8 -y
python3 -m venv ozon_venv
source ozon_venv/bin/activate
pip3 install -r requirements_app.txt
python3 app.py