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

    def insert(self, p_vaccine):
        self._conn.execute("""
               INSERT INTO vaccines (id, date, supplier, quantity)
               VALUES (?, ?, ?, ?)""",
                           [p_vaccine.id,
                            p_vaccine.date,
                            p_vaccine.supplier,
                            p_vaccine.quantity])

    def find(self, p_vaccine_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, date, supplier, quantity FROM vaccines WHERE id = ?
        """, [p_vaccine_id])

        return Vaccine(*c.fetchone())

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

    def insert(self, p_supplier):
        self._conn.execute("""
        INSERT INTO suppliers (id, name, logistic) VALUES (?, ?, ?)
        """, [p_supplier.id, p_supplier.name, p_supplier.logistic])

    def find(self, p_supplier_id):
        c = self._conn.cursor()
        c.execute("""SELECT id, name, logistic FROM suppliers WHERE id = ?""",
                  [p_supplier_id])
        return Supplier(*c.fetchone())

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

    def insert(self, p_clinic):
        self._conn.execute("""
        INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?, ?)
        """, [p_clinic.id, p_clinic.location, p_clinic.demand, p_clinic.logistic])

    def find(self, p_supplier_id):
        c = self._conn.cursor()
        c.execute(
        "SELECT id, location, demand, logistic FROM clinics WHERE id = ?",
            [p_supplier_id])
        return Clinic(*c.fetchone())

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

    def insert(self, p_logistic):
        self._conn.execute("""
        INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?, ?)
        """, [p_logistic.id,
              p_logistic.name,
              p_logistic.count_sent,
              p_logistic.count_received])

    def find(self, p_logistic_id):
        c = self._conn.cursor()
        c.execute("SELECT id, name, count_sent, count_received FROM logistics WHERE id=?",
                  [p_logistic_id])
        return Logistic(*c.fetchone())


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
        try:
            self.vaccines.create_table()
        except(e):
            pass
        try:
            self.suppliers.create_table()
        except(e):
            pass
        try:
            self.clinics.create_table()
        except(e):
            pass
        try:
            self.logistics.create_table()
        except(e):
            pass

# the repository singleton
dist_repo = _Repository()
atexit.register(dist_repo._close)
