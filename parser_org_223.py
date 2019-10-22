import datetime
import logging
import operator
import os
from functools import reduce
from warnings import filterwarnings

import pymysql

from connect_to_db import connect_bd

EXECUTE_PATH = os.path.dirname(os.path.abspath(__file__))
LOG_D = 'log_organization223'
LOG_DIR = f"{EXECUTE_PATH}/{LOG_D}"
TEMP_D = 'temp_organization223'
TEMP_DIR = f"{EXECUTE_PATH}/{TEMP_D}"
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
SUFFIX = '_from_ftp223'
SUFFIX_OD_CUS = ''
SUFFIX_ORG = ''
DB = 'tender'
filterwarnings('ignore', category=pymysql.Warning)
# file_log = './log_organization223/organization_ftp_223_' + str(datetime.date.today()) + '.log'
file_log = f'{LOG_DIR}/organization_ftp_223_{str(datetime.date.today())}.log'
logging.basicConfig(level=logging.DEBUG, filename=file_log,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def get_from_dict(data_dict, map_list):
    return reduce(operator.getitem, map_list, data_dict)


def logging_parser(*kwargs):
    s_log = f'{datetime.datetime.now()} '
    for i in kwargs:
        s_log += f'{i} '
    s_log += '\n\n'
    with open(file_log, 'a') as flog:
        flog.write(s_log)


def generator_univ(c):
    if type(c) == list:
        for i in c:
            yield i
    else:
        yield c


def get_el(d, *kwargs):
    try:
        res = get_from_dict(d, kwargs)
    except Exception:
        res = ''
    if res is None:
        res = ''
    return res


class Organization:
    log_update = 0
    log_insert = 0

    def __init__(self, org):
        self.org = org

    def get_org(self):
        try:
            orgs = generator_univ(self.org['nsiOrganization']['body']['item'])
        except Exception:
            orgs = []
        return orgs

    @staticmethod
    def guid(og):
        val = get_el(og, 'guid')
        return val

    @staticmethod
    def code(og):
        val = get_el(og, 'nsiOrganizationData', 'code')
        return val

    @staticmethod
    def inn(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'inn')
        return val

    @staticmethod
    def kpp(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'kpp')
        return val

    @staticmethod
    def ogrn(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'ogrn')
        return val

    @staticmethod
    def full_name(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'fullName')
        return val

    @staticmethod
    def short_name(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'shortName')
        return val

    @staticmethod
    def postal_address(og):
        val = get_el(og, 'nsiOrganizationData', 'mainInfo', 'postalAddress')
        return val

    @staticmethod
    def status(og):
        val = get_el(og, 'nsiOrganizationData', 'status')
        return val

    @staticmethod
    def phone(og):
        val = get_el(og, 'nsiOrganizationData', 'contactInfo', 'phone')
        return val

    @staticmethod
    def fax(og):
        val = get_el(og, 'nsiOrganizationData', 'contactInfo', 'fax')
        return val

    @staticmethod
    def email(og):
        val = get_el(og, 'nsiOrganizationData', 'contactInfo', 'email')
        return val

    @staticmethod
    def email(og):
        val = get_el(og, 'nsiOrganizationData', 'contactInfo', 'email')
        return val

    @staticmethod
    def contact_name(og):
        lastName = get_el(og, 'nsiOrganizationData', 'contactInfo', 'contactLastName')
        firstName = get_el(og, 'nsiOrganizationData', 'contactInfo', 'contactFirstName')
        middleName = get_el(og, 'nsiOrganizationData', 'contactInfo', 'contactMiddleName')
        return '{0} {1} {2}'.format(firstName, middleName, lastName)

    @staticmethod
    def okato(og):
        val = get_el(og, 'nsiOrganizationData', 'classification', 'okato')
        return val

    @staticmethod
    def oktmo(og):
        val = get_el(og, 'nsiOrganizationData', 'classification', 'oktmo')
        return val

    @staticmethod
    def query_in_customer(inn, kpp):
        con = connect_bd(DB)
        cur = con.cursor()
        cur.execute(
                f"""SELECT regNumber, region_code FROM od_customer{SUFFIX_OD_CUS} WHERE inn=%s AND kpp=%s""",
                (inn, kpp))
        res = cur.fetchone()
        cur.close()
        con.close()
        if res:
            return {'regNumber': res['regNumber'], 'region_code': res['region_code']}
        else:
            return {'regNumber': '', 'region_code': ''}

    @staticmethod
    def query_in_organizer(inn, kpp):
        con = connect_bd(DB)
        cur = con.cursor()
        cur.execute(f"""SELECT reg_num FROM organizer{SUFFIX_ORG} WHERE inn=%s AND kpp=%s""", (inn, kpp))
        res = cur.fetchone()
        cur.close()
        con.close()
        if res:
            return {'regNumber': res['reg_num']}
        else:
            return {'regNumber': ''}


def parser_o(org, path):
    code = Organization.guid(org)
    ogrn = Organization.ogrn(org)
    if not ogrn:
        logging_parser('У организации нет ogrn:', path)
        return
    contracts_count = 0
    contracts223_count = 0
    contracts_sum = 0.0
    contracts223_sum = 0.0
    regNumber = ''
    region_code = ''
    inn = Organization.inn(org)
    kpp = Organization.kpp(org)
    if inn and kpp:
        r_od = Organization.query_in_customer(inn, kpp)
        regNumber, region_code = r_od['regNumber'], r_od['region_code']
    if not (regNumber or not inn or not kpp):
        r_oo = Organization.query_in_organizer(inn, kpp)
        regNumber = r_oo['regNumber']
    full_name = Organization.full_name(org)
    short_name = Organization.short_name(org)
    postal_address = Organization.postal_address(org)
    phone = Organization.phone(org)
    fax = Organization.fax(org)
    email = Organization.email(org)
    contact_name = Organization.contact_name(org)
    okato = Organization.okato(org)
    oktmo = Organization.oktmo(org)
    status = Organization.status(org)
    con = connect_bd(DB)
    cur = con.cursor()
    cur.execute(f"""SELECT id FROM od_customer{SUFFIX} WHERE ogrn=%s""", (ogrn,))
    res_code = cur.fetchone()
    if res_code:
        query = f"""UPDATE od_customer{SUFFIX} SET regNumber=%s, 	inn=%s, kpp=%s, contracts_count=%s, contracts223_count=%s,
                contracts_sum=%s, contracts223_sum=%s, region_code=%s, full_name=%s, short_name=%s, postal_address=%s, 
                phone=%s, fax=%s, email=%s, contact_name=%s, code=%s, okato=%s, oktmo=%s, status = %s WHERE ogrn=%s"""
        value = (regNumber, inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum,
                 region_code, full_name, short_name, postal_address, phone, fax, email, contact_name, code, okato,
                 oktmo, status, ogrn)
        cur.execute(query, value)
        Organization.log_update += 1
    else:
        query1 = f"""INSERT INTO od_customer{SUFFIX} SET regNumber=%s, inn=%s, kpp=%s, contracts_count=%s, 
                contracts223_count=%s,
                contracts_sum=%s, contracts223_sum=%s, ogrn=%s, region_code=%s, full_name=%s, short_name=%s, postal_address=%s, 
                phone=%s, fax=%s, email=%s, contact_name=%s, code=%s, okato=%s, oktmo=%s, status = %s"""
        value1 = (regNumber, inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum, ogrn,
                  region_code, full_name, short_name, postal_address, phone, fax, email, contact_name, code, okato,
                  oktmo, status)
        cur.execute(query1, value1)
        Organization.log_insert += 1
    cur.close()
    con.close()


def parser(doc, path_xml):
    global file_log
    o = Organization(doc)
    orgs = o.get_org()
    if not orgs:
        logging_parser('В файле нет списка организаций:', path_xml)
    for org in orgs:
        try:
            parser_o(org, path_xml)
        except Exception:
            logging.exception(f'Ошибка парсера {path_xml}')
