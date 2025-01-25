import datetime
import ftplib
import logging
import operator
import os
import shutil
import sys
import time
import urllib
import uuid
import zipfile
from functools import reduce

import requests
import timeout_decorator
import xmltodict

import parser_org_223

TOKEN = 'edb04342-8607-49de-b5ee-ecc724fb220b'
types = ['nsiOrganization']
file_log = parser_org_223.file_log
temp_dir = parser_org_223.TEMP_DIR
# temp_dir = 'temp_organization223'
logging.basicConfig(level=logging.DEBUG, filename=file_log,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_xml_to_dict(filexml, dirxml):
    path_xml = dirxml + '/' + filexml
    l = os.stat(path_xml).st_size
    if l == 0:
        return
    with open(path_xml) as fd:
        try:
            s = fd.read()
            s = s.replace("ns2:", "")
            s = s.replace("oos:", "")
            doc = xmltodict.parse(s)
            parser_org_223.parser(doc, path_xml)

        except Exception as ex:
            logging.exception("Ошибка: ")
            parser_org_223.logging_parser('Ошибка конвертации в словарь', ex, path_xml)


def bolter(file, l_dir):
    try:
        get_xml_to_dict(file, l_dir)
    except Exception as exppars:
        logging.exception("Ошибка: ")
        parser_org_223.logging_parser('Не удалось пропарсить файл', exppars, file)

def get_list_api(period):
    count = 0
    while True:
        try:
            lf = []
            url = 'https://int44.zakupki.gov.ru/eis-integration/services/getDocsIP'
            unique_id = uuid.uuid4()
            current_datetime = datetime.datetime.utcnow().isoformat()
            request = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://zakupki.gov.ru/fz44/get-docs-ip/ws">
            <soapenv:Header>
            <individualPerson_token>{TOKEN}</individualPerson_token>
            </soapenv:Header>
            <soapenv:Body>
            <ws:getNsiRequest>
            <index>
            <id>{unique_id}</id>
            <createDateTime>{current_datetime}</createDateTime>
            <mode>PROD</mode>
            </index>
            <selectionParams>
            <nsiCode223>nsiCustomerRegistry</nsiCode223>
            <nsiKind>inc</nsiKind>
            </selectionParams>
            </ws:getNsiRequest>
            </soapenv:Body>
            </soapenv:Envelope>
            """
            headers = {
                'Content-Type': 'text/xml; charset=utf-8',
            }
            # print(request)
            response = requests.post(url, data=request, headers=headers)
            if response.status_code == 200:
                r = response.content.decode('utf-8')
                doc = xmltodict.parse(r)
                arlist = generator_univ(
                        get_el_list(doc, 'soap:Envelope', 'soap:Body', 'ns2:getNsiResponse',
                                    'dataInfo', 'nsiArchiveInfo'))
                for a in arlist:
                    u = get_el(a, 'archiveUrl')
                    lf.append(u)


            else:
                raise Exception('response code ' + response.status_code)
            return lf
        except Exception as ex:
            time.sleep(5)
            if count > 5:
                with open(file_log, 'a') as flog:
                    flog.write(
                            'Не удалось получить список архивов за ' + str(count) + ' попыток ' + str(
                                    ex))
                return []
            count += 1


def generator_univ(c):
    if c == "":
        raise StopIteration
    if type(c) == list:
        for i in c:
            yield i
    else:
        yield c


def get_from_dict(data_dict, map_list):
    return reduce(operator.getitem, map_list, data_dict)


def get_el(d, *kwargs):
    try:
        res = get_from_dict(d, kwargs)
    except Exception:
        res = ''
    if res is None:
        res = ''
    if type(res) is str:
        res = res.strip()
    return res


def get_el_list(d, *kwargs):
    try:
        res = get_from_dict(d, kwargs)
    except Exception as ex:
        res = []
    if res is None:
        res = []
    return res

@timeout_decorator.timeout(30)
def down_timeout(m):
    local_f = '{0}/{1}'.format(temp_dir, 'array.zip')
    opener = urllib.request.build_opener()
    opener.addheaders = [('individualPerson_token', TOKEN)]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(m, local_f)
    return local_f


def get_ar(m):
    """
    :param m: получаем имя архива
    :param path_parse1: получаем путь до архива
    :return: возвращаем локальный путь до архива или 0 в случае неудачи
    """
    retry = True
    count = 0
    while retry:
        try:
            lf = down_timeout(m)
            retry = False
            return lf
        except Exception as ex:
            time.sleep(5)
            if count > 5:
                with open(file_log, 'a') as flog:
                    flog.write(
                            'Не удалось скачать архив за ' + str(count) + ' попыток ' + str(ex) + ' ' + str(m) + '\n')
                return 0
            count += 1


def extract_org(m):
    l = get_ar(m)
    if l:
        # print(l)
        r_ind = l.rindex('.')
        l_dir = l[:r_ind]
        os.mkdir(l_dir)
        try:
            z = zipfile.ZipFile(l, 'r')
            z.extractall(l_dir)
            z.close()
        except UnicodeDecodeError as ea:
            parser_org_223.logging_parser('Не удалось извлечь архив', ea, l)
            try:
                os.system('unzip %s -d %s' % (l, l_dir))
            except Exception as ear:
                parser_org_223.logging_parser('Не удалось извлечь архив альтернативным методом', ear, l)
                return
        except Exception as e:
            logging.exception("Ошибка: ")
            parser_org_223.logging_parser('Не удалось извлечь архив', e, l)
            return

        try:
            file_list = os.listdir(l_dir)
        except Exception as ex:
            logging.exception("Ошибка: ")
            parser_org_223.logging_parser('Не удалось получить список файлов', ex, l_dir)
        else:
            for f in file_list:
                bolter(f, l_dir)
        os.remove(l)
        shutil.rmtree(l_dir, ignore_errors=True)


def main():
    with open(file_log, 'a') as flog:
        flog.write(f'Время начала работы парсера: {datetime.datetime.now()}\n')
    if len(sys.argv) == 1:
        print('Недостаточно параметров для запуска, используйте daily или last в качестве параметра')
        exit()
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.mkdir(temp_dir)
    try:
        if str(sys.argv[1]) == 'daily':
            arr_tenders = get_list_api('inc')
        elif str(sys.argv[1]) == 'last':
            arr_tenders = get_list_api('all')
        else:
            print('Неверное имя параметра, используйте daily или last в качестве параметра')
            arr_tenders = []
            exit()
        if not arr_tenders:
            parser_org_223.logging_parser('Получен пустой список архивов')
        for j in arr_tenders:
            try:
                extract_org(j)
            except Exception as exc:
                # print('Ошибка в экстракторе и парсере ' + str(exc) + ' ' + j)
                logging.exception("Ошибка в экстракторе и парсере: ")
                parser_org_223.logging_parser('Ошибка в экстракторе и парсере', exc, j)
                continue

    except Exception as ex:
        logging.exception("Ошибка: ")
        parser_org_223.logging_parser('Не удалось получить список архивов', ex)
    with open(parser_org_223.file_log, 'a') as flog:
        flog.write(f'Добавлено заказчиков Organization223 {parser_org_223.Organization.log_insert} \n')
        flog.write(f'Обновлено заказчиков Organization223 {parser_org_223.Organization.log_update} \n')
        flog.write(f'Время окончания работы парсера: {datetime.datetime.now()}\n\n\n')


if __name__ == "__main__":
    try:
        main()
    except Exception as exm:
        parser_org_223.logging_parser('Ошибка в функции main', exm)
