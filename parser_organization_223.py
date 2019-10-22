import datetime
import ftplib
import logging
import os
import shutil
import sys
import time
import zipfile
import xmltodict
import timeout_decorator
import parser_org_223
from connect_to_db import connect_bd

file_log = parser_org_223.file_log
temp_dir = parser_org_223.TEMP_DIR
# temp_dir = 'temp_organization223'
logging.basicConfig(level=logging.DEBUG, filename=file_log,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def get_xml_to_dict(filexml, dirxml):
    path_xml = dirxml + '/' + filexml
    # print(path_xml)
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


@timeout_decorator.timeout(300)
def down_timeout(m, path_parse1):
    host = 'ftp.zakupki.gov.ru'
    ftpuser = 'fz223free'
    password = 'fz223free'
    ftp2 = ftplib.FTP(host)
    ftp2.set_debuglevel(0)
    ftp2.encoding = 'utf8'
    ftp2.login(ftpuser, password)
    ftp2.cwd(path_parse1)
    local_f = '{0}/{1}'.format(temp_dir, str(m))
    lf = open(local_f, 'wb')
    ftp2.retrbinary('RETR ' + str(m), lf.write)
    lf.close()
    return local_f


def get_ar(m, path_parse1):
    global temp_dir
    retry = True
    count = 0
    while retry:
        try:
            lf = down_timeout(m, path_parse1)
            retry = False
            if count > 0:
                parser_org_223.logging_parser('Удалось скачать архив после попытки', count, m)
            return lf
        except Exception as ex:
            logging.exception("Ошибка: ")
            parser_org_223.logging_parser('Не удалось скачать архив', ex, m)
            if count > 100:
                parser_org_223.logging_parser('Не удалось скачать архив за 100 попыток', ex, m)
                return 0
            count += 1
            time.sleep(5)


def extract_org(m, path_parse1):
    l = get_ar(m, path_parse1)
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


def get_list_ftp_last(path_parse):
    host = 'ftp.zakupki.gov.ru'
    ftpuser = 'fz223free'
    password = 'fz223free'
    ftp2 = ftplib.FTP(host)
    ftp2.set_debuglevel(0)
    ftp2.encoding = 'utf8'
    ftp2.login(ftpuser, password)
    try:
        ftp2.cwd(path_parse)
    except Exception:
        return []
    data = ftp2.nlst()
    array_ar = []
    search_f = 'nsiOrganization'
    for i in data:
        if i.find(search_f) != -1:
            array_ar.append(i)

    return array_ar


def get_list_ftp_daily(path_parse):
    host = 'ftp.zakupki.gov.ru'
    ftpuser = 'fz223free'
    password = 'fz223free'
    ftp2 = ftplib.FTP(host)
    ftp2.set_debuglevel(0)
    ftp2.encoding = 'utf8'
    ftp2.login(ftpuser, password)
    try:
        ftp2.cwd(path_parse)
    except Exception:
        return []
    data = ftp2.nlst()
    array_ar = []
    con_arhiv = parser_org_223.connect_bd(parser_org_223.DB)
    cur_arhiv = con_arhiv.cursor()
    search_f = 'nsiOrganization'
    for i in data:
        if i.find(search_f) != -1:
            cur_arhiv.execute(
                    f"""SELECT id FROM arhiv_organization{parser_org_223.SUFFIX} WHERE arhiv = %s""",
                    (i,))
            find_file = cur_arhiv.fetchone()
            if find_file:
                continue
            else:
                array_ar.append(i)
                query_ar = f"""INSERT INTO arhiv_organization{parser_org_223.SUFFIX} SET arhiv = %s"""
                query_par = (i,)
                cur_arhiv.execute(query_ar, query_par)
    cur_arhiv.close()
    con_arhiv.close()
    ftp2.close()
    return array_ar


def main():
    with open(file_log, 'a') as flog:
        flog.write(f'Время начала работы парсера: {datetime.datetime.now()}\n')
    if len(sys.argv) == 1:
        print('Недостаточно параметров для запуска, используйте daily или last в качестве параметра')
        exit()
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.mkdir(temp_dir)
    path_parse = ''
    try:
        if str(sys.argv[1]) == 'daily':
            path_parse = '/out/nsi/nsiOrganization/daily'
            arr_tenders = get_list_ftp_daily(path_parse)
        elif str(sys.argv[1]) == 'last':
            path_parse = '/out/nsi/nsiOrganization'
            arr_tenders = get_list_ftp_last(path_parse)
        else:
            print('Неверное имя параметра, используйте daily или last в качестве параметра')
            arr_tenders = []
            exit()
        if not arr_tenders:
            parser_org_223.logging_parser('Получен пустой список архивов', path_parse)
        for j in arr_tenders:
            try:
                extract_org(j, path_parse)
            except Exception as exc:
                # print('Ошибка в экстракторе и парсере ' + str(exc) + ' ' + j)
                logging.exception("Ошибка в экстракторе и парсере: ")
                parser_org_223.logging_parser('Ошибка в экстракторе и парсере', exc, j)
                continue

    except Exception as ex:
        logging.exception("Ошибка: ")
        parser_org_223.logging_parser('Не удалось получить список архивов', ex, path_parse)
    with open(parser_org_223.file_log, 'a') as flog:
        flog.write(f'Добавлено заказчиков Organization223 {parser_org_223.Organization.log_insert} \n')
        flog.write(f'Обновлено заказчиков Organization223 {parser_org_223.Organization.log_update} \n')
        flog.write(f'Время окончания работы парсера: {datetime.datetime.now()}\n\n\n')


if __name__ == "__main__":
    try:
        main()
    except Exception as exm:
        parser_org_223.logging_parser('Ошибка в функции main', exm)
