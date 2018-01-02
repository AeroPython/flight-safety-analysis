"""
Queries to retrieve data from database
"""

import sqlite3
import pandas as pd

from .utils import convert_lat, convert_lon, rename_categories, \
    combine_date_time

DATE_FORMAT = '%m/%d/%y %H:%M:%S'
TIME_FORMAT = '%H%M'

FAR_PARTS = "'121 ', '125 '"

EVENTS_COLUMNS = (
    'ev_id',
    'ntsb_no',
    'ev_type',
    'ev_date',
    'ev_time',
    'ev_tmzn',
    'ev_city',
    'ev_state',
    'ev_country',
    'ev_site_zipcode',
    'mid_air',
    'on_ground_collision',
    'latitude',
    'longitude',
    'latlong_acq',
    'apt_name',
    'ev_nr_apt_id',
    'ev_nr_apt_loc',
    'apt_dist',
    'apt_dir',
    'apt_elev',
    'wx_brief_comp',
    'wx_src_iic',
    'wx_obs_time',
    'wx_obs_dir',
    'wx_obs_fac_id',
    'wx_obs_elev',
    'wx_obs_dist',
    'wx_obs_tmzn',
    'light_cond',
    'sky_cond_nonceil',
    'sky_nonceil_ht',
    'sky_ceil_ht',
    'sky_cond_ceil',
    'vis_rvr',
    'vis_rvv',
    'vis_sm',
    'wx_temp',
    'wx_dew_pt',
    'wind_dir_deg',
    'wind_dir_ind',
    'wind_vel_kts',
    'wind_vel_ind',
    'gust_ind',
    'gust_kts',
    'altimeter',
    'wx_dens_alt',
    'wx_int_precip',
    'metar',
    'ev_highest_injury',
    'inj_f_grnd',
    'inj_m_grnd',
    'inj_s_grnd',
    'inj_tot_f',
    'inj_tot_m',
    'inj_tot_n',
    'inj_tot_s',
    'inj_tot_t',
    'invest_agy',
    'ntsb_docket',
    'ntsb_notf_from',
    'ntsb_notf_date',
    'ntsb_notf_tm',
    'fiche_number',
    'wx_cond_basic',
    'faa_dist_office'
)

EVENTS_NUMERIC = (
    "apt_elev",
    "apt_dir",
    "gust_kts",
    "inj_f_grnd",
    "inj_m_grnd",
    "inj_s_grnd",
    "inj_tot_f",
    "inj_tot_m",
    "inj_tot_n",
    "inj_tot_s",
    "inj_tot_t",
    "vis_rvv",
    "wind_dir_deg",
    "wx_obs_dist",
    "wx_obs_time",
    "wx_dew_pt",
    "wx_obs_dir",
    "wx_temp",
    "altimeter",
    "apt_dist",
    "vis_rvr",
    "vis_sm",
    "ntsb_docket",
    "sky_ceil_ht",
    "sky_nonceil_ht",
    "wx_obs_elev",
    "wx_dens_alt"
)

EVENTS_CATEGORICAL = (
    "ev_highest_injury",
    "ev_nr_apt_loc",
    "ev_state",
    "ev_tmzn",
    "ev_type",
    "gust_ind",
    "invest_agy",
    "latlong_acq",
    "light_cond",
    "mid_air",
    "on_ground_collision",
    "sky_cond_ceil",
    "sky_cond_nonceil",
    "wind_dir_ind",
    "wind_vel_ind",
    "wx_brief_comp",
    "wx_cond_basic",
    "wx_int_precip",
    "wx_src_iic"
)

AIRCRAFT_COLUMNS = (
    "ev_id",
    "Aircraft_Key",
    "regis_no",
    "ntsb_no",
    "acft_missing",
    "far_part",
    "flt_plan_filed",
    "flight_plan_activated",
    "damage",
    "acft_fire",
    "acft_expl",
    "acft_make",
    "acft_model",
    "acft_series",
    "acft_serial_no",
    "cert_max_gr_wt",
    "acft_category",
    "acft_reg_cls",
    "homebuilt",
    "date_last_insp",
    "afm_hrs",
    "afm_hrs_last_insp",
    "commercial_space_flight",
    "unmanned",
    "ifr_equipped_cert",
    "elt_mounted_aircraft",
    "elt_connected_antenna",
    "afm_hrs_since",
    "air_medical",
    "certs_held",
    "dest_apt_id",
    "dest_country",
    "dest_same_local",
    "dest_state",
    "dprt_apt_id",
    "dprt_country",
    "dprt_pt_same_ev",
    "dprt_state",
    "dprt_timezn",
    "elt_aided_loc_ev",
    "elt_install",
    "elt_oper",
    "elt_type",
    "evacuation",
    "fixed_retractable",
    "oper_addr_same",
    "oper_cert",
    "oper_code",
    "oper_country",
    "oper_dom_int",
    "oper_individual_name",
    "oper_pax_cargo",
    "oper_same",
    "oper_sched",
    "oper_state",
    "oprtng_cert",
    "owner_country",
    "owner_state",
    "report_to_icao",
    "second_pilot",
    "site_seeing",
    "type_fly",
    "type_last_insp",
    "dest_city",
    "dprt_city",
    "med_type_flight",
    "oper_cert_num",
    "oper_city",
    "oper_dba",
    "oper_name",
    "oper_street",
    "oper_zip",
    "owner_acft",
    "owner_city",
    "owner_street",
    "owner_zip",
    "rwy_num",
    "fuel_on_board",
    "elt_manufacturer",
    "elt_model",
    "elt_reason_other",
    "cc_seats",
    "fc_seats",
    "pax_seats",
    "phase_flt_spec",
    "rwy_len",
    "rwy_width",
    "acft_year",
    "dprt_time",
    "total_seats",
    "num_eng"
)

AIRCRAFT_CATEGORICAL = (
    "commercial_space_flight",
    "unmanned",
    "ifr_equipped_cert",
    "elt_mounted_aircraft",
    "elt_connected_antenna",
    "acft_category",
    "acft_expl",
    "acft_fire",
    "acft_missing",
    "flt_plan_filed",
    "homebuilt",
    "afm_hrs_since",
    "air_medical",
    "certs_held",
    "damage",
    "dest_apt_id",
    "dest_country",
    "dest_same_local",
    "dest_state",
    "dprt_apt_id",
    "dprt_country",
    "dprt_pt_same_ev",
    "dprt_state",
    "dprt_timezn",
    "elt_aided_loc_ev",
    "elt_install",
    "elt_oper",
    "elt_type",
    "evacuation",
    "far_part",
    "fixed_retractable",
    "flight_plan_activated",
    "oper_addr_same",
    "oper_cert",
    "oper_code",
    "oper_country",
    "oper_dom_int",
    "oper_individual_name",
    "oper_pax_cargo",
    "oper_same",
    "oper_sched",
    "oper_state",
    "oprtng_cert",
    "owner_country",
    "owner_state",
    "report_to_icao",
    "second_pilot",
    "site_seeing",
    "type_fly",
    "type_last_insp",
    "acft_make",
    "acft_model",
    "acft_series",
    "dest_city",
    "dprt_city",
    "med_type_flight",
    "oper_cert_num",
    "oper_city",
    "oper_dba",
    "oper_name",
    "oper_street",
    "oper_zip",
    "owner_acft",
    "owner_city",
    "owner_street",
    "owner_zip",
    "rwy_num",
    "fuel_on_board",
    "elt_manufacturer",
    "elt_model",
    "elt_reason_other"
)

AIRCRAFT_NUMERIC = (
    "Aircraft_Key",
    "cc_seats",
    "cert_max_gr_wt",
    "fc_seats",
    "pax_seats",
    "rwy_len",
    "rwy_width",
    "acft_year",
    "afm_hrs",
    "afm_hrs_last_insp",
    "dprt_time",
    "total_seats",
    "num_eng",
    "phase_flt_spec",
)

OCCURRENCES_COLUMNS = (
    "ev_id",
    "Aircraft_Key",
    "Occurrence_No",
    "Occurrence_Code",
    "Phase_of_Flight",
    "Altitude"
)

OCCURENCES_NUMERIC = (
    "Aircraft_Key",
    "Occurrence_No",
    "Altitude",
    "Phase_of_Flight"
)

OCCURRENCE_CATEGORICAL = (
    "Occurrence_Code",
    "Phase_of_Flight",
)

FLIGHT_TIME_COLS = (
    "ev_id",
    "Aircraft_Key",
    "crew_no",
    "flight_type",
    "flight_craft",
    "flight_hours"
)

FLIGHT_TIME_NUMERIC = (
    "Aircraft_Key",
    "crew_no",
    "flight_hours"
)

FLIGHT_TIME_CATEGORICAL = (
    "flight_type",
    "flight_craft"
)

SEQ_OF_EVETNS_COLUMNS = (
    "ev_id",
    "Aircraft_Key",
    "Occurrence_No",
    "seq_event_no",
    "group_code",
    "Subj_Code",
    "Cause_Factor",
    "Modifier_Code",
    "Person_Code",
)

SEQ_OF_EVENTS_NUMERIC = (
    "Aircraft_Key",
    "Occurrence_No",
    "seq_event_no"
)

SEQ_OF_EVENTS_CATEGORICAL = (
    "group_code",
    "Subj_Code",
    "Cause_Factor",
    "Modifier_Code",
    "Person_Code",
)

FLIGHT_CREW_COLS = (
    'ev_id',
    'Aircraft_Key',
    'crew_no',
    'crew_category',
    'crew_age',
    'crew_sex'
)

FLIGHT_CREW_NUMERIC = (
    'crew_no',
    'crew_age'
)

FLIGHT_CREW_CATEGORICAL = (
    'crew_category',
    'crew_sex'
)


def get_codes_meaning(con, table, column):
    query = (
        "select distinct code_iaids, meaning from eADMSPUB_DataDictionary "
        f"where \"Table\"='{table}' and \"Column\"='{column}'"
    )
    return pd.read_sql(query, con, index_col='code_iaids')


def get_occurrences_accidents(con):
    occurrence_cols = ", ".join(OCCURRENCES_COLUMNS)

    query = (
        f"SELECT {occurrence_cols} FROM Occurrences WHERE ev_id IN "
        "(SELECT ev_id FROM events WHERE ev_type='ACC' AND "
        "ev_date IS NOT NULL) AND ev_id IN (SELECT ev_id FROM aircraft "
        f"WHERE far_part in ({FAR_PARTS}))"
    )

    occurrences = pd.read_sql(query, con)

    for c in OCCURENCES_NUMERIC:
        occurrences[c] = pd.to_numeric(occurrences[c], errors='coerce')

    # phase_flt_spec is parsed as numeric and this is used to get phases of
    # flight with less detail (ie. "Takeoff - roll/run" -> "Takeoff)
    occurrences['phase_flt_spec_gross'] = ((occurrences.Phase_of_Flight // 10) * 10)

    for c in list(OCCURRENCE_CATEGORICAL) + ['phase_flt_spec_gross']:
        occurrences[c] = occurrences[c].astype('category')

    PHASE_FLT_SPEC = get_codes_meaning(con, 'Occurrences', 'Phase_of_Flight')
    OCCU_CODE_SPEC = get_codes_meaning(con, 'Occurrences', 'Occurrence_Code')

    cats = rename_categories(occurrences['Phase_of_Flight'].cat.categories,
                             PHASE_FLT_SPEC)
    occurrences['Phase_of_Flight'].cat.rename_categories(cats, inplace=True)

    cats = rename_categories(occurrences['phase_flt_spec_gross'].cat.categories,
                             PHASE_FLT_SPEC)
    occurrences['phase_flt_spec_gross'].cat.rename_categories(cats, inplace=True)

    cats = rename_categories(occurrences['Occurrence_Code'].cat.categories,
                             OCCU_CODE_SPEC)
    occurrences['Occurrence_Code'].cat.rename_categories(cats, inplace=True)

    return occurrences


def get_flight_time_accidents(con):
    flight_time_cols = ", ".join(FLIGHT_TIME_COLS)

    query = (
        f"SELECT {flight_time_cols} FROM flight_time WHERE ev_id IN "
        "(SELECT ev_id FROM events WHERE ev_type='ACC' AND "
        "ev_date IS NOT NULL) AND ev_id IN (SELECT ev_id FROM aircraft WHERE "
        f"far_part in ({FAR_PARTS}))"
    )

    flight_time = pd.read_sql(query, con)

    for c in FLIGHT_TIME_NUMERIC:
        flight_time[c] = pd.to_numeric(flight_time[c], errors='coerce')

    for c in FLIGHT_TIME_CATEGORICAL:
        flight_time[c] = flight_time[c].astype('category')

    return flight_time


def get_seq_of_events_accidents(con):
    seq_of_events_cols = ", ".join(SEQ_OF_EVETNS_COLUMNS)

    query = (
        f"SELECT {seq_of_events_cols} FROM seq_of_events WHERE ev_id IN "
        "(SELECT ev_id FROM events WHERE ev_type='ACC' AND "
        "ev_date IS NOT NULL) AND ev_id IN (SELECT ev_id FROM aircraft WHERE "
        f"far_part IN ({FAR_PARTS}))"
    )

    seq_of_events = pd.read_sql(query, con)

    # DROP GROUP_CODE = 0 because it is not codified
    # seq_of_events = seq_of_events[seq_of_events.group_code != 0]

    for c in SEQ_OF_EVENTS_NUMERIC:
        seq_of_events[c] = pd.to_numeric(seq_of_events[c], errors='coerce')

    for c in SEQ_OF_EVENTS_CATEGORICAL:
        seq_of_events[c] = seq_of_events[c].astype('category')

    return seq_of_events


def get_flight_crew_accidents(con):

    flight_crew_cols = ', '.join(FLIGHT_CREW_COLS)
    
    query = (
        f"SELECT {flight_crew_cols} FROM Flight_Crew WHERE ev_id IN "
        "(SELECT ev_id FROM events WHERE ev_type='ACC' AND "
        "ev_date IS NOT NULL) AND ev_id IN (SELECT ev_id FROM aircraft "
        f"WHERE far_part in ({FAR_PARTS}))"
    )
    flight_crew = pd.read_sql_query(query, con)

    for c in FLIGHT_CREW_NUMERIC:
        flight_crew[c] = pd.to_numeric(flight_crew[c], errors='coerce')

    for c in FLIGHT_CREW_CATEGORICAL:
        flight_crew[c] = flight_crew[c].astype('category')

    return flight_crew


class AvallDB:

    def __init__(self, file, acc=True, inc=False, far_parts='ALL'):

        self.con = sqlite3.connect(file)

        self._acc = acc
        self._inc = inc
        self._far_parts = far_parts

        # Index matching conditions above. If None when executing a query,
        # they will be gathered.
        self._ev_ids = None

    def set_filtering_conditions(self, acc=None, inc=None, far_parts=None):

        if acc:
            self._acc = acc
        if inc:
            self._inc = inc
        if far_parts:
            self._far_parts = far_parts

        # If conditions have changed, ev_ids must be gathered again
        self._ev_ids = None

        current_filters = {'acc': self._acc,
                           'inc': self._inc,
                           'far_parts': self._far_parts}

        return current_filters

    def _get_ev_ids_ev_type_query(self):

        if self._acc and self._inc:
            return ("SELECT ev_id FROM events WHERE ev_type='ACC' OR " 
                    "ev_type='INC'")

        elif self._acc:
            return "SELECT ev_id FROM events WHERE ev_type='ACC'"

        else:
            return "SELECT ev_id FROM events WHERE ev_type='INC'"

    def _get_ev_ids_far_part_query(self):

        if isinstance(self._far_parts, str) and self._far_parts.upper() == 'ALL':
            return "SELECT ev_id FROM aircraft"

        else:
            # Convert to string and add trailing space and enclose with ''
            far_parts_ = ["'" + str(fp).strip() + ' ' + "'" for fp in
                          self._far_parts]

            # transform list to a string separated by ,
            far_parts_ = "(" + ", ".join(far_parts_) + ")"
            return ("SELECT ev_id FROM aircraft WHERE far_part IN "
                    f"{far_parts_}")

    def _get_conditions_query(self):

        selections = [self._get_ev_ids_ev_type_query(),
                      self._get_ev_ids_far_part_query()]

        conditions = " AND ".join([f"ev_id IN ({sel})" for sel in selections])

        return conditions

    def _set_matching_ev_ids(self):

        # Get conditions query to be written after WHERE
        conds = self._get_conditions_query()
        # Write query
        query = f"SELECT ev_id FROM events WHERE {conds}"
        # Get events matching conditions
        ev_ids = pd.read_sql(query, self.con)['ev_id'].values

        # Transform strings matching to string
        self._ev_ids = "'" + "', '".join(ev_ids) + "'"

    def _execute_query(self, query, **kwargs):

        if self._ev_ids is None:
            self._set_matching_ev_ids()

        query = query + f" WHERE ev_id in ({self._ev_ids})"

        return pd.read_sql(query, self.con, index_col='ev_id', **kwargs)

    def get_events(self):
        ev_cols_ = ", ".join(EVENTS_COLUMNS)
        query = (f"SELECT {ev_cols_} FROM events ")

        events = self._execute_query(
            query,
            parse_dates={'ev_date': DATE_FORMAT,
                         'ev_time': TIME_FORMAT,
                         'ntsb_notf_date': DATE_FORMAT,
                         'ntsb_notf_tm': TIME_FORMAT
                         }
        )

        # Substitute ev_date and ev_time for a datetime col: ev_date_time
        events['ev_date_time'] = events.apply(
            combine_date_time, 1, args=('ev_date', 'ev_time')
            )
        events.drop(['ev_date', 'ev_time'], axis=1, inplace=True)

        # Idem with ntsb_notf_date_tm
        events['ntsb_notf_date_tm'] = events.apply(
            combine_date_time, 1, args=('ntsb_notf_date', 'ntsb_notf_tm')
            )
        events.drop(['ntsb_notf_date', 'ntsb_notf_tm'], axis=1, inplace=True)

        for c in EVENTS_NUMERIC:
            events[c] = pd.to_numeric(events[c], errors='coerce')

        for c in EVENTS_CATEGORICAL:
            events[c] = events[c].astype('category')

        events['latitude'] = events['latitude'].apply(convert_lat)
        events['longitude'] = events['longitude'].apply(convert_lon)

        return events

    def get_aircrafts(self):
        ac_columns = ", ".join(AIRCRAFT_COLUMNS)

        query = f"SELECT {ac_columns} FROM aircraft"

        aircrafts = self._execute_query(
            query, parse_dates={'date_last_insp': DATE_FORMAT}
            )

        for c in AIRCRAFT_NUMERIC:
            aircrafts[c] = pd.to_numeric(aircrafts[c], errors='coerce')

        # phase_flt_spec is parsed as numeric and this is used to get phases of
        # flight with less detail (ie. "Takeoff - roll/run" -> "Takeoff)
        aircrafts['phase_flt_spec_gross'] = (
                (aircrafts.phase_flt_spec // 10) * 10)

        new_categorical_cols = ['phase_flt_spec_gross', 'phase_flt_spec']
        for c in list(AIRCRAFT_CATEGORICAL) + new_categorical_cols:
            aircrafts[c] = aircrafts[c].astype('category')

        PHASE_FLT_SPEC_DICT = get_codes_meaning(
            self.con, 'aircraft', 'phase_flt_spec'
        )

        # Change codes for names (ie. 570 to Landing)
        cats = rename_categories(
            aircrafts['phase_flt_spec_gross'].cat.categories,
            PHASE_FLT_SPEC_DICT)
        aircrafts['phase_flt_spec_gross'].cat.rename_categories(cats,
                                                                inplace=True)

        cats = rename_categories(aircrafts['phase_flt_spec'].cat.categories,
                                 PHASE_FLT_SPEC_DICT)
        aircrafts['phase_flt_spec'].cat.rename_categories(cats, inplace=True)

        return aircrafts
