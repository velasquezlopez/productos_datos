install_requirements:
	python3 -m pip install -r requirements.txt

simulate:
ifdef input_date
	python3 simulate.py $(input_date)
else
	python3 simulate.py
endif

data_processing:
	python3 data_processing.py

load_data:
	python3 load_data.py