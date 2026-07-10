"""Génération de données industrielles synthétiques pour PostgreSQL."""

import os
import random
import time
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values


RANDOM_SEED = 42

MACHINE_TARGET = 50
TECHNICIAN_TARGET = 20
SENSOR_TARGET = 150

PRODUCTION_ROWS = 20_000
MAINTENANCE_ROWS = 2_000
ALERT_ROWS = 5_000


def get_connection():
    """Créer une connexion PostgreSQL à partir des variables Docker."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "industrial_db"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
    )


def wait_for_postgres(max_attempts=20, delay=3):
    """Attendre que PostgreSQL soit disponible."""

    for attempt in range(1, max_attempts + 1):
        try:
            connection = get_connection()
            connection.close()
            print("PostgreSQL est disponible.")
            return
        except psycopg2.OperationalError:
            print(
                f"Tentative {attempt}/{max_attempts} : "
                "PostgreSQL n'est pas encore disponible."
            )
            time.sleep(delay)

    raise RuntimeError("Connexion à PostgreSQL impossible.")


def generate_machines(cursor):
    """Compléter la table machines jusqu'à 50 machines."""

    cursor.execute("SELECT COUNT(*) FROM industrial.machines;")
    current_count = cursor.fetchone()[0]

    if current_count >= MACHINE_TARGET:
        print(f"Machines déjà présentes : {current_count}")
        return

    machine_types = [
        "Crusher",
        "Conveyor",
        "Washer",
        "Dryer",
        "Pump",
        "Compressor",
        "Packaging Machine",
    ]

    manufacturers = [
        "Industrial Systems",
        "Mining Equipment",
        "Process Engineering",
        "Thermal Solutions",
        "Smart Manufacturing",
    ]

    statuses = ["ACTIVE", "ACTIVE", "ACTIVE", "MAINTENANCE", "INACTIVE"]

    rows = []

    for number in range(current_count + 1, MACHINE_TARGET + 1):
        rows.append(
            (
                f"Machine-{number:03d}",
                random.choice(machine_types),
                random.randint(1, 5),
                date.today() - timedelta(days=random.randint(365, 5000)),
                random.choice(statuses),
                random.choice(manufacturers),
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.machines (
            machine_name,
            machine_type,
            workshop_id,
            installation_date,
            status,
            manufacturer
        )
        VALUES %s
        ON CONFLICT (machine_name) DO NOTHING;
        """,
        rows,
    )

    print(f"{len(rows)} machines générées.")


def generate_technicians(cursor):
    """Compléter la table technicians jusqu'à 20 techniciens."""

    cursor.execute("SELECT COUNT(*) FROM industrial.technicians;")
    current_count = cursor.fetchone()[0]

    if current_count >= TECHNICIAN_TARGET:
        print(f"Techniciens déjà présents : {current_count}")
        return

    first_names = [
        "Amine",
        "Salma",
        "Mehdi",
        "Nadia",
        "Hamza",
        "Lina",
        "Anas",
        "Meryem",
        "Ayoub",
        "Kawtar",
    ]

    last_names = [
        "Amrani",
        "Tazi",
        "Bennani",
        "Chraibi",
        "Filali",
        "Naciri",
        "Tahiri",
        "Lahlou",
        "Fassi",
        "Zerouali",
    ]

    specialties = [
        "Mécanique industrielle",
        "Électricité industrielle",
        "Automatisme",
        "Instrumentation",
        "Maintenance générale",
    ]

    rows = []

    for number in range(current_count + 1, TECHNICIAN_TARGET + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        rows.append(
            (
                first_name,
                last_name,
                random.choice(specialties),
                f"technician{number}@industrial.local",
                f"06{number:08d}",
                date.today() - timedelta(days=random.randint(100, 4000)),
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.technicians (
            first_name,
            last_name,
            specialty,
            email,
            phone,
            hire_date
        )
        VALUES %s
        ON CONFLICT (email) DO NOTHING;
        """,
        rows,
    )

    print(f"{len(rows)} techniciens générés.")


def generate_sensors(cursor):
    """Compléter la table sensors jusqu'à 150 capteurs."""

    cursor.execute("SELECT COUNT(*) FROM industrial.sensors;")
    current_count = cursor.fetchone()[0]

    if current_count >= SENSOR_TARGET:
        print(f"Capteurs déjà présents : {current_count}")
        return

    cursor.execute(
        "SELECT machine_id FROM industrial.machines ORDER BY machine_id;"
    )
    machine_ids = [row[0] for row in cursor.fetchall()]

    sensor_configuration = {
        "TEMPERATURE": ("°C", 0, 100),
        "VIBRATION": ("mm/s", 0, 15),
        "PRESSURE": ("bar", 0, 20),
        "ENERGY": ("kWh", 0, 600),
    }

    rows = []

    for number in range(current_count + 1, SENSOR_TARGET + 1):
        sensor_type = random.choice(list(sensor_configuration))
        unit, minimum, maximum = sensor_configuration[sensor_type]

        rows.append(
            (
                f"SENSOR-{number:04d}",
                random.choice(machine_ids),
                sensor_type,
                unit,
                minimum,
                maximum,
                "ACTIVE",
                date.today() - timedelta(days=random.randint(30, 1500)),
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.sensors (
            sensor_code,
            machine_id,
            sensor_type,
            measurement_unit,
            minimum_threshold,
            maximum_threshold,
            status,
            installation_date
        )
        VALUES %s
        ON CONFLICT (sensor_code) DO NOTHING;
        """,
        rows,
    )

    print(f"{len(rows)} capteurs générés.")


def generate_production(cursor):
    """Générer l'historique de production."""

    cursor.execute("SELECT COUNT(*) FROM industrial.production;")

    if cursor.fetchone()[0] > 0:
        print("Les données de production existent déjà.")
        return

    cursor.execute(
        "SELECT machine_id FROM industrial.machines ORDER BY machine_id;"
    )
    machine_ids = [row[0] for row in cursor.fetchall()]

    rows = []

    for _ in range(PRODUCTION_ROWS):
        quantity = round(random.uniform(100, 1500), 3)
        rejected = round(quantity * random.uniform(0, 0.08), 3)

        rows.append(
            (
                random.choice(machine_ids),
                date.today() - timedelta(days=random.randint(0, 730)),
                random.choice(["MORNING", "AFTERNOON", "NIGHT"]),
                quantity,
                round(random.uniform(50, 900), 3),
                round(random.uniform(1, 24), 2),
                rejected,
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.production (
            machine_id,
            production_date,
            shift,
            quantity_produced,
            energy_consumption,
            operating_hours,
            rejected_quantity
        )
        VALUES %s;
        """,
        rows,
        page_size=2000,
    )

    print(f"{PRODUCTION_ROWS} lignes de production générées.")


def generate_maintenance(cursor):
    """Générer les interventions de maintenance."""

    cursor.execute("SELECT COUNT(*) FROM industrial.maintenance;")

    if cursor.fetchone()[0] > 0:
        print("Les données de maintenance existent déjà.")
        return

    cursor.execute("SELECT machine_id FROM industrial.machines;")
    machine_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT technician_id FROM industrial.technicians;")
    technician_ids = [row[0] for row in cursor.fetchall()]

    rows = []

    for _ in range(MAINTENANCE_ROWS):
        start_time = datetime.now() - timedelta(
            days=random.randint(0, 730),
            hours=random.randint(0, 23),
        )

        duration = timedelta(hours=random.uniform(1, 48))
        status = random.choice(
            ["PLANNED", "IN_PROGRESS", "COMPLETED", "CANCELLED"]
        )

        end_time = start_time + duration if status == "COMPLETED" else None

        rows.append(
            (
                random.choice(machine_ids),
                random.choice(technician_ids),
                random.choice(["PREVENTIVE", "CORRECTIVE", "EMERGENCY"]),
                start_time,
                end_time,
                "Intervention industrielle générée automatiquement",
                round(random.uniform(100, 20_000), 2),
                status,
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.maintenance (
            machine_id,
            technician_id,
            maintenance_type,
            start_time,
            end_time,
            description,
            cost,
            status
        )
        VALUES %s;
        """,
        rows,
        page_size=1000,
    )

    print(f"{MAINTENANCE_ROWS} interventions générées.")


def generate_alerts(cursor):
    """Générer les alertes issues des capteurs."""

    cursor.execute("SELECT COUNT(*) FROM industrial.alerts;")

    if cursor.fetchone()[0] > 0:
        print("Les alertes existent déjà.")
        return

    cursor.execute(
        """
        SELECT
            sensor_id,
            machine_id,
            sensor_type,
            maximum_threshold
        FROM industrial.sensors;
        """
    )

    sensors = cursor.fetchall()
    rows = []

    for _ in range(ALERT_ROWS):
        sensor_id, machine_id, sensor_type, maximum_threshold = random.choice(
            sensors
        )

        threshold = float(maximum_threshold or 100)
        measured_value = round(
            threshold * random.uniform(0.75, 1.40),
            3,
        )

        severity = random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"])
        alert_status = random.choice(["OPEN", "ACKNOWLEDGED", "RESOLVED"])

        alert_timestamp = datetime.now() - timedelta(
            days=random.randint(0, 365),
            minutes=random.randint(0, 1440),
        )

        resolved_at = (
            alert_timestamp + timedelta(hours=random.randint(1, 72))
            if alert_status == "RESOLVED"
            else None
        )

        rows.append(
            (
                sensor_id,
                machine_id,
                alert_timestamp,
                f"{sensor_type}_THRESHOLD",
                severity,
                measured_value,
                threshold,
                f"Dépassement détecté sur le capteur {sensor_id}",
                alert_status,
                resolved_at,
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO industrial.alerts (
            sensor_id,
            machine_id,
            alert_timestamp,
            alert_type,
            severity,
            measured_value,
            threshold_value,
            message,
            status,
            resolved_at
        )
        VALUES %s;
        """,
        rows,
        page_size=1000,
    )

    print(f"{ALERT_ROWS} alertes générées.")


def main():
    """Exécuter toutes les étapes de génération."""

    random.seed(RANDOM_SEED)
    wait_for_postgres()

    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                generate_machines(cursor)
                generate_technicians(cursor)
                generate_sensors(cursor)
                generate_production(cursor)
                generate_maintenance(cursor)
                generate_alerts(cursor)

        print("Génération terminée avec succès.")

    except Exception as error:
        connection.rollback()
        print(f"Erreur pendant la génération : {error}")
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()