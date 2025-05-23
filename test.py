import csv
from datetime import datetime
import oracledb

conn = oracledb.connect(
    user="inf2ns_sosnowskij",
    password="jacek",
    dsn="213.184.8.44/orcl"
)

cursor = conn.cursor()

def validate_row(row):
    if not row['pesel'].isdigit() or len(row['pesel']) != 11:
        with open("./errors.log", "a") as errfile:
            errfile.write(f"{datetime.now()} - NIEPRAWIDLOWY PESEL: {row['pesel']}\n")
        return False
    return True


def load_clients(file_path):
    with open(file_path, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count_added = 0
        count_skipped = 0
        for row in reader:
            pesel = row['pesel']
            print(f"Sprawdzam PESEL: '{pesel}'")
            if not validate_row(row):
                print(f"PESEL niepoprawny lub odrzucony: '{pesel}'")
                count_skipped += 1
                continue

            try:
                cursor.execute("SELECT COUNT(*) FROM Klienci WHERE pesel = :pesel", {'pesel': pesel})
                (count,) = cursor.fetchone()
                if count > 0:
                    print(f"PESEL już w bazie: '{pesel}' – pomijam")
                    count_skipped += 1
                    continue

                cursor.execute("""
                    INSERT INTO Klienci (imie, nazwisko, email, telefon, pesel)
                    VALUES (:1, :2, :3, :4, :5)
                """, (row['imie'], row['nazwisko'], row['email'], row['telefon'], pesel))
                conn.commit()
                print(f"Dodano klienta z PESEL: '{pesel}'")
                count_added += 1

            except Exception as e:
                with open("data/errors.log", "a") as errfile:
                    errfile.write(f"{datetime.now()} - KLIENT - {e}\n")
                conn.rollback()

        print(f"Klientów dodano: {count_added}, pominięto: {count_skipped}")


def load_depart(file_path):
    with open(file_path, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM Nieruchomosci 
                    WHERE adres = :adres AND miasto = :miasto
                """, {'adres': row['adres'], 'miasto': row['miasto']})
                (count,) = cursor.fetchone()
                if count > 0:
                    continue

                cursor.execute("""
                    INSERT INTO Nieruchomosci (adres, miasto, powierzchnia, cena, typ)
                    VALUES (:1, :2, :3, :4, :5)
                """, (
                    row['adres'], row['miasto'],
                    float(row['powierzchnia']),
                    float(row['cena']),
                    row['typ']
                ))
                conn.commit()
            except Exception as e:
                with open("data/errors.log", "a") as errfile:
                    errfile.write(f"{datetime.now()} - NIERUCHOMOSC - {e}\n")
                conn.rollback()

def load_trans(file_path):
    with open(file_path, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                data = datetime.strptime(row['data_sprzedazy'], "%Y-%m-%d")

                cursor.execute("""
                    SELECT COUNT(*) FROM Transakcje 
                    WHERE id_klienta = :idk AND id_nieruchomosci = :idn AND data_sprzedazy = :data
                """, {
                    'idk': int(row['id_klienta']),
                    'idn': int(row['id_nieruchomosci']),
                    'data': data
                })
                (count,) = cursor.fetchone()
                if count > 0:
                    continue

                cursor.execute("""
                    INSERT INTO Transakcje (id_klienta, id_nieruchomosci, data_sprzedazy, cena_sprzedazy)
                    VALUES (:1, :2, :3, :4)
                """, (
                    int(row['id_klienta']),
                    int(row['id_nieruchomosci']),
                    data,
                    float(row['cena_sprzedazy'])
                ))
                conn.commit()
            except Exception as e:
                with open("data/errors.log", "a") as errfile:
                    errfile.write(f"{datetime.now()} - TRANSAKCJA - {e}\n")
                conn.rollback()


load_clients("./klienci.csv")
print("Zakończono wczytywanie klientów.")
cursor.execute("SELECT COUNT(*) FROM Klienci")
print("Liczba klientów w bazie:", cursor.fetchone()[0])

load_depart("./nieruchomosci.csv")
load_trans("./transakcje.csv")

cursor.close()
conn.close()
