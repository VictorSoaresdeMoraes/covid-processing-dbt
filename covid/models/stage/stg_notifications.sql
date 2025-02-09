WITH staged_data AS (
SELECT
    t.$1:"@timestamp"::datetime AS timestamp,
    
    t.$1:municipioNotificacaoIBGE::bigint AS notification_ibge_city_code,
    t.$1:municipioNotificacao::varchar(100) AS notification_city_name,
    t.$1:estadoNotificacaoIBGE::int AS notification_ibge_state_code,
    t.$1:estadoNotificacao::varchar(100) AS notification_state_name,

    t.$1:municipioIBGE::bigint AS patient_residence_ibge_city_code,
    t.$1:municipio::varchar(100) AS patient_residence_city_name,
    t.$1:estadoIBGE::int AS patient_residence_ibge_state_code,
    t.$1:estado::varchar(100) AS patient_residence_state_name,

    t.$1:profissionalSaude::varchar(3) AS patient_is_health_professional,
    t.$1:sexo::varchar(15) AS patient_gender,
    t.$1:idade::int AS patient_age,
    t.$1:racaCor::varchar(30) AS patient_race,
    t.$1:condicoes::varchar(200) AS patient_medical_conditions,
    CASE
        WHEN REPLACE(t.$1:codigoRecebeuVacina, '"', '') = 1
            THEN 'Sim'
        WHEN REPLACE(t.$1:codigoRecebeuVacina, '"', '') = 2
            THEN 'Não'
        WHEN REPLACE(t.$1:codigoRecebeuVacina, '"', '') = 3
            THEN 'Não Respondeu'
    END
        ::varchar(15) AS patient_is_vaccinated,
    t.$1:dataPrimeiraDose::datetime AS patient_first_dose_date,
    t.$1:dataSegundaDose::datetime AS patient_second_dose_date,
    
    ARRAY_SIZE(t.$1:codigoDosesVacina)::variant AS quantity_vaccine_doses,

    t.$1:dataInicioSintomas::datetime AS symptoms_start_date,
    t.$1:sintomas::varchar(200) AS symptoms,
    t.$1:dataNotificacao::datetime AS notification_date,
    t.$1:evolucaoCaso::varchar(50) AS case_evolution,
    t.$1:dataEncerramento::datetime AS case_end_date,

    CASE
        WHEN t.$1:codigoEstrategiaCovid = 1
            THEN 'Diagnóstico assistencial (sintomático)'
        WHEN t.$1:codigoEstrategiaCovid = 2
            THEN 'Busca ativa de assintomático'
        WHEN t.$1:codigoEstrategiaCovid = 3
            THEN 'Triagem de população específica'
        ELSE 'Outro'
    END::varchar(50) AS covid_strategy_diagnostic,
    
    CASE
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 1
            THEN 'Serviço de saúde(UBS, hospital,UPA etc.)'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 2
            THEN 'Local de trabalho'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 3
            THEN 'Aeroporto'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 4
            THEN 'Farmácia ou drogaria'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 5
            THEN 'Escola'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 6
            THEN 'Domicílio ou comunidade'
        WHEN REPLACE(t.$1:codigoLocalRealizacaoTestagem, '"', '') = 7
            THEN 'Outro'
        END::varchar(50)
        AS location_test_code,

    PARSE_JSON(t.$1:testes) AS testes
    
FROM @raw.gcs_covid/notifications/full/notifications
(FILE_FORMAT => raw.parquet_format) t
)

SELECT 
    timestamp, 
    notification_ibge_city_code,
    notification_city_name,
    notification_ibge_state_code,
    notification_state_name,
    patient_residence_ibge_city_code,
    patient_residence_city_name,
    patient_residence_ibge_state_code,
    patient_residence_state_name,
    patient_is_health_professional,
    patient_gender,
    patient_age,
    patient_race,
    patient_medical_conditions,
    patient_is_vaccinated,
    patient_first_dose_date,
    patient_second_dose_date, 
    quantity_vaccine_doses,
    symptoms_start_date,
    symptoms,
    notification_date,
    case_evolution,
    case_end_date,
    covid_strategy_diagnostic,
    location_test_code,

    value:dataColetaTeste:iso::STRING AS test_date,
    value:estadoTeste::STRING AS test_status,
    INITCAP(value:fabricanteTeste)::STRING AS test_manufacter,
    value:loteTeste::STRING AS test_batch_code,
    value:resultadoTeste::STRING AS test_result,
    INITCAP(value:tipoTeste)::STRING AS test_type
    
FROM staged_data,
LATERAL FLATTEN(input => testes)