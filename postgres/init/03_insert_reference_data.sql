SET search_path TO industrial;

-- =========================================================
-- ATELIERS
-- =========================================================

INSERT INTO workshops (
    workshop_name,
    location,
    description
)
VALUES
    (
        'Extraction',
        'Khouribga',
        'Extraction et préparation initiale du phosphate'
    ),
    (
        'Concassage',
        'Khouribga',
        'Réduction de la taille des matières premières'
    ),
    (
        'Lavage',
        'Khouribga',
        'Lavage et enrichissement du phosphate'
    ),
    (
        'Séchage',
        'Jorf Lasfar',
        'Réduction de l’humidité du produit'
    ),
    (
        'Conditionnement',
        'Safi',
        'Préparation et conditionnement du produit final'
    )
ON CONFLICT (workshop_name) DO NOTHING;

-- =========================================================
-- MACHINES
-- =========================================================

INSERT INTO machines (
    machine_name,
    machine_type,
    workshop_id,
    installation_date,
    status,
    manufacturer
)
VALUES
    (
        'Crusher-01',
        'Crusher',
        2,
        '2018-03-15',
        'ACTIVE',
        'Industrial Systems'
    ),
    (
        'Conveyor-01',
        'Conveyor',
        1,
        '2019-06-20',
        'ACTIVE',
        'Mining Equipment'
    ),
    (
        'Washer-01',
        'Washer',
        3,
        '2020-01-10',
        'ACTIVE',
        'Process Engineering'
    ),
    (
        'Dryer-01',
        'Dryer',
        4,
        '2017-11-05',
        'MAINTENANCE',
        'Thermal Solutions'
    ),
    (
        'Packaging-01',
        'Packaging Machine',
        5,
        '2021-09-17',
        'ACTIVE',
        'Smart Packaging'
    )
ON CONFLICT (machine_name) DO NOTHING;

-- =========================================================
-- TECHNICIENS
-- =========================================================

INSERT INTO technicians (
    first_name,
    last_name,
    specialty,
    email,
    phone,
    hire_date
)
VALUES
    (
        'Ahmed',
        'El Mansouri',
        'Mécanique industrielle',
        'ahmed.elmansouri@industrial.local',
        '0600000001',
        '2018-04-01'
    ),
    (
        'Sara',
        'Benali',
        'Électricité industrielle',
        'sara.benali@industrial.local',
        '0600000002',
        '2019-07-15'
    ),
    (
        'Youssef',
        'Alaoui',
        'Automatisme',
        'youssef.alaoui@industrial.local',
        '0600000003',
        '2020-02-10'
    ),
    (
        'Imane',
        'Idrissi',
        'Instrumentation',
        'imane.idrissi@industrial.local',
        '0600000004',
        '2021-01-05'
    ),
    (
        'Omar',
        'Berrada',
        'Maintenance générale',
        'omar.berrada@industrial.local',
        '0600000005',
        '2017-10-20'
    )
ON CONFLICT (email) DO NOTHING;

-- =========================================================
-- CAPTEURS
-- =========================================================

INSERT INTO sensors (
    sensor_code,
    machine_id,
    sensor_type,
    measurement_unit,
    minimum_threshold,
    maximum_threshold,
    status,
    installation_date
)
VALUES
    (
        'TEMP-CRUSHER-01',
        1,
        'TEMPERATURE',
        '°C',
        0,
        90,
        'ACTIVE',
        '2022-01-10'
    ),
    (
        'VIB-CRUSHER-01',
        1,
        'VIBRATION',
        'mm/s',
        0,
        12,
        'ACTIVE',
        '2022-01-10'
    ),
    (
        'ENERGY-CONVEYOR-01',
        2,
        'ENERGY',
        'kWh',
        0,
        500,
        'ACTIVE',
        '2022-03-15'
    ),
    (
        'PRESS-WASHER-01',
        3,
        'PRESSURE',
        'bar',
        0,
        15,
        'ACTIVE',
        '2022-05-20'
    ),
    (
        'TEMP-DRYER-01',
        4,
        'TEMPERATURE',
        '°C',
        20,
        150,
        'ACTIVE',
        '2022-06-12'
    ),
    (
        'VIB-PACKAGING-01',
        5,
        'VIBRATION',
        'mm/s',
        0,
        10,
        'ACTIVE',
        '2023-02-01'
    )
ON CONFLICT (sensor_code) DO NOTHING;