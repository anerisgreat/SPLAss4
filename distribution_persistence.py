import sqlite3
import atexit

#Vaccines
class Vaccine(object):
    def __init__(self, p_id, p_date, p_supplier, p_quantity):
        self.id = p_id
        self.date = p_date
        self.supplier = p_supplier
        self.quantity = p_quantity

class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def create_table(self):
        self._conn.executescript("""
        CREATE TABLE vaccines(
        id          INTEGER PRIMARY KEY,
        date        DATE    NOT NULL,
        supplier    INTEGER REFERENCES supplier(id),
        quantity    INTEGER NOT NULL
        )""")

    def drop_table(self):
        self._conn.execute("DROP TABLE vaccines")

    def insert(self, p_vaccine):
        self._conn.execute("""INSERT INTO vaccines
            (id, date, supplier, quantity)
            VALUES (?, ?, ?, ?)""",
                [p_vaccine.id,
                    p_vaccine.date,
                    p_vaccine.supplier,
                    p_vaccine.quantity])

    def update(self, p_vaccine):
        self._conn.execute("""UPDATE vaccines
            SET date = ?, supplier = ?, quantity = ?
            WHERE id = ?""",
                [p_vaccine.date,
                    p_vaccine.supplier,
                    p_vaccine.quantity,
                    p_vaccine.id])

    def find(self, p_vaccine_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, date, supplier, quantity FROM vaccines WHERE id = ?
        """, [p_vaccine_id])

        return Vaccine(*c.fetchone())

    def get_maxid(self):
        c = self._conn.cursor()
        c.execute("SELECT MAX(id) from vaccines")
        return c.fetchone()[0]

    def get_all_by_date_generator(self):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, date, supplier, quantity FROM vaccines
            ORDER BY date""")

        tup = c.fetchone()
        while(tup is not None):
            yield Vaccine(*tup)
            tup = c.fetchone()

    def del_by_id(self, p_id):
        self._conn.execute("DELETE FROM vaccines WHERE id = ?", [p_id])

    def get_total_quantity(self):
        c = self._conn.cursor()
        c.execute("SELECT SUM(quantity) FROM vaccines")
        return c.fetchone()[0]

#Supplier
class Supplier(object):
    def __init__(self, p_id, p_name, p_logistic):
        self.id = p_id
        self.name = p_name
        self.logistic = p_logistic

class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def create_table(self):
        self._conn.executescript("""
        CREATE TABLE suppliers(
        id          INTEGER PRIMARY KEY,
        name        STRING  NOT NULL,
        logistic    INTEGER REFERENCES logistics(id)
        )""")

    def drop_table(self):
        self._conn.execute("DROP TABLE suppliers")

    def insert(self, p_supplier):
        self._conn.execute("""INSERT INTO suppliers
            (id, name, logistic) VALUES (?, ?, ?)""",
            [p_supplier.id, p_supplier.name, p_supplier.logistic])

    def update(self, p_supplier):
        self._conn.execute("""UPDATE suppliers
            SET name = ?, logistic = ? WHERE id = ?""",
            [p_supplier.name, p_supplier.logistic, p_supplier.id])

    def find(self, p_supplier_id):
        c = self._conn.cursor()
        c.execute("""SELECT id, name, logistic FROM suppliers WHERE id = ?""",
                  [p_supplier_id])
        return Supplier(*c.fetchone())

    def find_by_name(self, p_supplier_name):
        c = self._conn.cursor()
        c.execute("""SELECT id, name, logistic FROM suppliers WHERE name = ?""",
                  [p_supplier_name])
        return Supplier(*c.fetchone())

    def get_maxid(self):
        c = self._conn.cursor()
        c.execute("SELECT MAX(id) from suppliers")
        return c.fetchone()[0]

#Clinics
class Clinic(object):
    def __init__(self, p_id, p_location, p_demand, p_logistic):
        self.id = p_id
        self.location = p_location
        self.demand = p_demand
        self.logistic = p_logistic

class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def create_table(self):
        self._conn.executescript("""
        CREATE TABLE clinics(
        id          INTEGER PRIMARY KEY,
        location    STRING  NOT NULL,
        demand      INTEGER NOT NULL,
        logistic    INTEGER REFERENCES logistics(id)
        )""")

    def drop_table(self):
        self._conn.execute("DROP TABLE clinics")

    def insert(self, p_clinic):
        self._conn.execute("""INSERT INTO clinics
            (id, location, demand, logistic) VALUES (?, ?, ?, ?)""",
            [p_clinic.id,
             p_clinic.location,
             p_clinic.demand,
             p_clinic.logistic])

    def update(self, p_clinic):
        self._conn.execute("""UPDATE clinics
            SET location = ?, demand = ?, logistic = ?
            WHERE id = ?""",
            [p_clinic.location,
             p_clinic.demand,
             p_clinic.logistic,
             p_clinic.id])

    def find(self, p_clinic_id):
        c = self._conn.cursor()
        c.execute(
        "SELECT id, location, demand, logistic FROM clinics WHERE id = ?",
            [p_clinic_id])
        return Clinic(*c.fetchone())

    def find_by_location(self, p_clinic_location):
        c = self._conn.cursor()
        c.execute(
        "SELECT id, location, demand, logistic FROM clinics WHERE location = ?",
            [p_clinic_location])
        return Clinic(*c.fetchone())

    def get_maxid(self):
        c = self._conn.cursor()
        c.execute("SELECT MAX(id) from clinics")
        return c.fetchone()[0]

    def get_total_demand(self):
        c = self._conn.cursor()
        c.execute("SELECT SUM(demand) FROM clinics")
        return c.fetchone()[0]

#Logistics
class Logistic(object):
    def __init__(self, p_id, p_name, p_count_sent, p_count_received):
        self.id = p_id
        self.name = p_name
        self.count_sent = p_count_sent
        self.count_received = p_count_received

class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def create_table(self):
        self._conn.executescript("""
        CREATE TABLE logistics(
        id             INTEGER PRIMARY KEY,
        name           STRING  NOT NULL,
        count_sent     INTEGER NOT NULL,
        count_received INTEGER NOT NULL
        )""")

    def drop_table(self):
        self._conn.execute("DROP TABLE logistics")

    def insert(self, p_logistic):
        self._conn.execute("""INSERT INTO logistics
            (id, name, count_sent, count_received) VALUES (?, ?, ?, ?)""",
            [p_logistic.id,
              p_logistic.name,
              p_logistic.count_sent,
              p_logistic.count_received])

    def update(self, p_logistic):
        self._conn.execute("""UPDATE logistics
        SET name = ?, count_sent = ?, count_received = ? WHERE id = ?""",
                           [p_logistic.name,
                            p_logistic.count_sent,
                            p_logistic.count_received,
                            p_logistic.id])

    def find(self, p_logistic_id):
        c = self._conn.cursor()
        c.execute("""SELECT id, name, count_sent, count_received
                    FROM logistics
                    WHERE id=?""",
                  [p_logistic_id])
        return Logistic(*c.fetchone())

    def get_maxid(self):
        c = self._conn.cursor()
        c.execute("SELECT MAX(id) from logistics")
        return c.fetchone()[0]

    def get_total_sent_received(self):
        c = self._conn.cursor()
        c.execute("SELECT SUM(count_sent), SUM(count_received) FROM logistics")
        return c.fetchone()

#The Repository
class _Repository(object):
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self.vaccines.create_table()
        self.suppliers.create_table()
        self.clinics.create_table()
        self.logistics.create_table()

    def drop_tables(self):
        self.vaccines.drop_table()
        self.suppliers.drop_table()
        self.clinics.drop_table()
        self.logistics.drop_table()

# the repository singleton
dist_repo = _Repository()
atexit.register(dist_repo._close)
