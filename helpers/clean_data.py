import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import pandas as pd

cluster = SLURMCluster(cores=4, processes=1, memory="100GB", job_extra_directives=['--time=06:00:00'])
client = Client(cluster)
cluster.scale(jobs=4)

### For more info refer to the notebook

def format_hour(hour_series):
    hour_series = hour_series.str.upper() + "M"
    hour_series = hour_series.str.replace(r'^00', '12', regex=True)
    return dd.to_datetime(hour_series, format="%I%M%p", errors="coerce")

def pad_parking_days(parking_days):
    parking_days = parking_days.replace(' ', 'B')
    return parking_days.ljust(7, 'B')

try:
  # Define the base path of the Parquet file
  base_path = '/d/hpc/projects/FRI/bigdata/students/cb17769/'

  # Read the Parquet file into a Dask DataFrame
  dataset = dd.read_parquet(f'{base_path}dataset.parquet')

  length_dataset = dataset.shape[0].compute()
  print(f"Dataset has {length_dataset} rows")
  # 140423664

  ## Violation
  # Dates
  # Fist observation datetime
  #dataset['time_first_observed'] = format_hour(dataset['time_first_observed'])
#
  ## Date first observed
  #dataset['date_first_observed'] = dataset['date_first_observed'].replace({0: None, '0': None})
  #dataset['date_first_observed'] = dataset['date_first_observed'].replace('0001-01-03T12:00:00.000', None)
  #dataset['date_first_observed'] = dd.to_datetime(dataset['date_first_observed'], format='%Y%m%d', errors='coerce')
  #
  ## merge Datetime first observed
  #dataset['date_first_observed'] = dd.to_datetime(dataset["date_first_observed"].dt.strftime('%Y-%m-%d') + ' ' + dataset["time_first_observed"].dt.strftime('%H:%M:%S'))
  #dataset = dataset.drop(["time_first_observed"], axis=1)

  # Issue date
  dataset['issue_date'] = dd.to_datetime(dataset["issue_date"], format="mixed")
  dataset['violation_time'] = format_hour(dataset['violation_time'])

  # merge Datetime issue date
  dataset['issue_date'] = dd.to_datetime(dataset["issue_date"].dt.strftime('%Y-%m-%d') + ' ' + dataset["violation_time"].dt.strftime('%H:%M:%S'))
  dataset = dataset.drop(["violation_time"], axis=1)

  # Location and codes
  dataset['violation_code'] = dataset['violation_code'].fillna(0).astype(int)
  dataset['violation_code'] = dataset['violation_code'].replace({0: None})

  # Violation county
  county_to_borough = {
      "BRONX": "BX", # Bronx
      "BX": "BX",
      "Bronx": "BX",
      "BRONX": "BX",
      "BK": "K", # Brooklyn known as Kings
      "K": "K",
      "Kings": "K",
      "KINGS": "K",
      "KING": "K",
      "Q": "Q", # Queens
      "QN": "Q",
      "Qns": "Q",
      "QUEEN": "Q",
      "QUEENS": "Q",
      "QNS": "Q",
      "QU": "Q",
      "NY": "NY", # Manhattan known as New York
      "MN": "NY",
      "MAN": "NY",
      "NEW Y": "NY",
      "NEWY": "NY",
      "NYC": "NY",
      "ST": "R", # Staten Island known as Richmond
      "R": "R",
      "Rich": "R",
      "RICH": "R",
      "RICHM": "R",
      "RC": "R",
      "MH": "NY",
      "MS": "NY",
      "N": "NY",
      "P": "NY",
      "PBX": "NY",
      "USA": "NY",
      "VINIS": "NY",
      "A": None,
      "F": None,
      "ABX": None,
      "108": None,
      "103": "R", # Staten Island zip code
      "00000": None,
      "K   F": "K",
  }

  dataset['violation_county'] = dataset['violation_county'].replace(county_to_borough)

  dataset = dataset.dropna(subset=['violation_county'])

  dataset['street_code'] = dataset['street_code1'].where(dataset['street_code1'] != 0, dataset['street_code2'].where(dataset['street_code2'] != 0, dataset['street_code3'])).astype("string")
  dataset['street_code'] = dataset['street_code'].replace({'0': None, 0: None})
  dataset = dataset.drop(["street_code1", "street_code2", "street_code3"], axis=1)

  # Location is the same as violation_precint
  #dataset = dataset.drop(["violation_location"], axis=1)
  
  ## Vehicle
  #dataset['vehicle_make'] = dataset['vehicle_make'].str.upper()
  
  # Dates
  current_year = 2024
  dataset['vehicle_year'] = dataset['vehicle_year'].astype(int)
  dataset['vehicle_year'] = dataset['vehicle_year'].replace({0: None})
  dataset['vehicle_year'] = dataset['vehicle_year'].where(dataset['vehicle_year'] <= current_year, None)
  
  dataset['vehicle_expiration_date'] = pd.to_datetime(dataset['vehicle_expiration_date'], format='%Y%m%d', errors='coerce')

  ## Vehicle Registration
 
  def check_registration_state(df):
    codes = "AL ALABAMA MT MONTANA AK ALASKA NE NEBRASKA AZ ARIZONA NV NEVADA AR ARKANSAS NH NEW HAMPSHIRE CA CALIFORNIA NJ NEW JERSEY CO COLORADO NM NEW MEXICO CT CONNECTICUT NY NEW YORK DE DELAWARE NC NORTH CAROLINA FL FLORIDA ND NORTH DAKOTA GA GEORGIA OH OHIO HI HAWAII OK OKLAHOMA ID IDAHO OR OREGON IL ILLINOIS PA PENNSYLVANIA IN INDIANA RI RHODE ISLAND IA IOWA SC SOUTH CAROLINA KS KANSAS SD SOUTH DAKOTA KY KENTUCKY TN TENNESSEE LA LOUISIANA TX TEXAS ME MAINE UT UTAH MD MARYLAND VT VERMONT MA MASSACHUSETTS VA VIRGINIA MI MICHIGAN WA WASHINGTON MN MINNESOTA WV WEST VIRGINIA MS MISSISSIPPI WI WISCONSIN MO MISSOURI WY WYOMING AB Alberta ON Ontario BC British Columbia PE Prince Edward Island FO Foreign QB Quebec MB Manitoba SK Saskatchewan MX Mexico GV U.S. Government NB New Brunswick DP U.S. State Dept. NF Newfoundland DC Washington D.C. NT Northwest Territories YT Yukon Territory NS Nova Scotia"
    codes = codes.split()
    codes = [code for code in codes if code.isupper() and len(code) == 2]
    df['registration_state'] = df['registration_state'].apply(lambda x: x if x in codes else None)
    return df
  
  dataset['registration_state'] = dataset['registration_state'].map_partitions(check_registration_state)

  def check_plate_type(df):
    plate_types = "AGR Agricultural Vehicle MCD Motorcycle Dealer AMB Ambulance MCL Marine Corps League ARG Air National Guard MED Medical Doctor ATD All Terrain Deale MOT Motorcycle ATV All Terrain Vehicle NLM Naval Militia AYG Army National Guard NYA New York Assembly BOB Birthplace of Baseball NYC New York City Council BOT Boat NYS New York Senate CBS County Bd. of Supervisors OMF Omnibus Public Service CCK County Clerk OML Livery CHC  Household Carrier (Com) OMO Omnibus Out-of-State CLG County Legislators OMR Bus CMB Combination - Connecticut OMS Rental CME  Coroner Medical Examiner OMT Taxi CMH Congress. Medal of Honor OMV Omnibus Vanity COM Commercial Vehicle ORC Organization (Com) CSP Sports (Com) ORG Organization (Pas) DLR Dealer PAS Passenger EDU Educator PHS Pearl Harbor Survivors FAR Farm vehicle PPH Purple Heart FPW Former Prisoner of War PSD Political Subd. (Official) GAC Governor's Additional Car RGC Regional (Com) GFC Gift Certificate RGL Regional (Pas) GSC Governor's Second Car SCL School Car GSM Gold Star Mothers SNO Snowmobile HAC Ham Operator Comm SOS Survivors of the Shield HAM Ham Operator SPC Special Purpose Comm. HIF Special Reg.Hearse SPO Sports HIR Hearse Coach SRF Special Passenger - Vanity HIS Historical Vehicle SRN Special Passenger - Judges HOU House/Coach Trailer STA State Agencies HSM Historical Motorcycle STG State National Guard IRP Intl. Registration Plan SUP Justice Supreme Court ITP In Transit Permit TOW Tow Truck JCA Justice Court of Appeals TRA Transporter JCL Justice Court of Claims THC Household Carrier Tractor JSC Supreme Court Appl. Div TRC Tractor Regular JWV Jewish War Veterans TRL Trailer Regular LMA Class A Limited Use Mtrcyl. USC U. S. Congress LMB Class B Limited Use Mtrcyl. USS U. S. Senate LMC Class C Limited Use Mtrcyl. VAS Voluntary Ambulance Svc. LOC Locomotive VPL Van Pool LTR Light Trailer WUG World University Games LUA Limited Use Automobile"
    plate_types = plate_types.split()
    plate_types = [plate_id for plate_id in plate_types if plate_id.isupper() and len(plate_id) == 3]
    df['plate_type'] = df['plate_type'].apply(lambda x: x if x in plate_types else None)
    return df

  dataset['plate_type'] = dataset['plate_type'].map_partitions(check_plate_type)

  # Plate ID
  # Upper case, not blank plate, between 5 and 8 characters, only letters and numbers
  #dataset['plate_id'] = dataset['plate_id'].str.upper()
  #dataset['plate_id'] = dataset['plate_id'].replace('BLANKPLATE', None)
  #dataset['plate_id'] = dataset['plate_id'].where(dataset['plate_id'].str.len() >= 5, None)
  #dataset['plate_id'] = dataset['plate_id'].where(dataset['plate_id'].str.len() <= 8, None)
  #dataset['plate_id'] = dataset['plate_id'].where(dataset['plate_id'].str.match(r'^[A-Z0-9]*$'), None)

  ## Issuer and law
  #dataset['issuer_precinct'] = dataset['issuer_precinct'].astype(int)
  #dataset['issuer_precinct'] = dataset['issuer_precinct'].replace({0: None, '0': None})
  #dataset['issuer_code'] = dataset['issuer_code'].astype(int)
  #dataset['issuer_code'] = dataset['issuer_code'].replace({0: None, '0': None})
#
  #dataset['law_section'] = dataset['law_section'].astype(int)
  #dataset['law_section'] = dataset['law_section'].replace({0: None, '0': None})

  # parking days
  dataset['days_parking_in_effect'] = dataset['days_parking_in_effect'].fillna('')
  dataset['days_parking_in_effect'] = dataset['days_parking_in_effect'].map_partitions(lambda x: x.apply(pad_parking_days))

  # hours in effect
  #dataset['from_hours_in_effect'] = format_hour(dataset['from_hours_in_effect']).dt.time
  #dataset['to_hours_in_effect'] = format_hour(dataset['to_hours_in_effect']).dt.time
  
  # unregistered vehicle
  dataset['unregistered_vehicle'] = dataset['unregistered_vehicle'].notna() & (dataset['unregistered_vehicle'] == '0')
  #dataset['unregistered_vehicle'] = dataset['unregistered_vehicle'].astype(int) 
  
  #dataset['meter_number'] = dataset['meter_number'].replace({'0': None, 0: None})

  # compute
  dataset = dataset.persist()
  print("Data computed")

  # Save the data as a new Parquet file
  dataset.to_parquet(f'{base_path}cleaned_data.parquet', compression='snappy')
  print("Data cleaning complete")

finally:
  client.close()
  cluster.close()