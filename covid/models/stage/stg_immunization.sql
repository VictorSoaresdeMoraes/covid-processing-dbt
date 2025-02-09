with staged_data AS (
SELECT
    t.$1:"@timestamp"::datetime AS timestamp,
    
    t.$1:estabelecimento_uf::char(2) AS immunization_state_uf,
    CASE 
        WHEN t.$1:estabelecimento_uf = 'AC' THEN 'Acre'
        WHEN t.$1:estabelecimento_uf = 'AL' THEN 'Alagoas'
        WHEN t.$1:estabelecimento_uf = 'AP' THEN 'Amapá'
        WHEN t.$1:estabelecimento_uf = 'AM' THEN 'Amazonas'
        WHEN t.$1:estabelecimento_uf = 'BA' THEN 'Bahia'
        WHEN t.$1:estabelecimento_uf = 'CE' THEN 'Ceará'
        WHEN t.$1:estabelecimento_uf = 'DF' THEN 'Federal District'
        WHEN t.$1:estabelecimento_uf = 'ES' THEN 'Espírito Santo'
        WHEN t.$1:estabelecimento_uf = 'GO' THEN 'Goiás'
        WHEN t.$1:estabelecimento_uf = 'MA' THEN 'Maranhão'
        WHEN t.$1:estabelecimento_uf = 'MT' THEN 'Mato Grosso'
        WHEN t.$1:estabelecimento_uf = 'MS' THEN 'Mato Grosso do Sul'
        WHEN t.$1:estabelecimento_uf = 'MG' THEN 'Minas Gerais'
        WHEN t.$1:estabelecimento_uf = 'PA' THEN 'Pará'
        WHEN t.$1:estabelecimento_uf = 'PB' THEN 'Paraíba'
        WHEN t.$1:estabelecimento_uf = 'PR' THEN 'Paraná'
        WHEN t.$1:estabelecimento_uf = 'PE' THEN 'Pernambuco'
        WHEN t.$1:estabelecimento_uf = 'PI' THEN 'Piauí'
        WHEN t.$1:estabelecimento_uf = 'RJ' THEN 'Rio de Janeiro'
        WHEN t.$1:estabelecimento_uf = 'RN' THEN 'Rio Grande do Norte'
        WHEN t.$1:estabelecimento_uf = 'RS' THEN 'Rio Grande do Sul'
        WHEN t.$1:estabelecimento_uf = 'RO' THEN 'Rondônia'
        WHEN t.$1:estabelecimento_uf = 'RR' THEN 'Roraima'
        WHEN t.$1:estabelecimento_uf = 'SC' THEN 'Santa Catarina'
        WHEN t.$1:estabelecimento_uf = 'SP' THEN 'São Paulo'
        WHEN t.$1:estabelecimento_uf = 'SE' THEN 'Sergipe'
        WHEN t.$1:estabelecimento_uf = 'TO' THEN 'Tocantins'
    END::varchar(100) AS immunization_state_name,
    t.$1:estabelecimento_municipio_codigo::bigint AS immunization_city_ibge_code,
    INITCAP(t.$1:estabelecimento_municipio_nome)::varchar(100) AS immunization_city_name,
    INITCAP(t.$1:estalecimento_noFantasia)::varchar(255) AS facility_trade_name,
    t.$1:estabelecimento_valor::varchar AS facility_cnes_code,
    INITCAP(t.$1:estabelecimento_razaoSocial)::varchar(255) AS facility_legal_name,

    INITCAP(t.$1:paciente_endereco_nmPais)::varchar(30) AS patient_coutry_name,
    t.$1:paciente_endereco_uf::char(2) AS patient_state_uf,
    CASE 
        WHEN t.$1:paciente_endereco_uf = 'AC' THEN 'Acre'
        WHEN t.$1:paciente_endereco_uf = 'AL' THEN 'Alagoas'
        WHEN t.$1:paciente_endereco_uf = 'AP' THEN 'Amapá'
        WHEN t.$1:paciente_endereco_uf = 'AM' THEN 'Amazonas'
        WHEN t.$1:paciente_endereco_uf = 'BA' THEN 'Bahia'
        WHEN t.$1:paciente_endereco_uf = 'CE' THEN 'Ceará'
        WHEN t.$1:paciente_endereco_uf = 'DF' THEN 'Federal District'
        WHEN t.$1:paciente_endereco_uf = 'ES' THEN 'Espírito Santo'
        WHEN t.$1:paciente_endereco_uf = 'GO' THEN 'Goiás'
        WHEN t.$1:paciente_endereco_uf = 'MA' THEN 'Maranhão'
        WHEN t.$1:paciente_endereco_uf = 'MT' THEN 'Mato Grosso'
        WHEN t.$1:paciente_endereco_uf = 'MS' THEN 'Mato Grosso do Sul'
        WHEN t.$1:paciente_endereco_uf = 'MG' THEN 'Minas Gerais'
        WHEN t.$1:paciente_endereco_uf = 'PA' THEN 'Pará'
        WHEN t.$1:paciente_endereco_uf = 'PB' THEN 'Paraíba'
        WHEN t.$1:paciente_endereco_uf = 'PR' THEN 'Paraná'
        WHEN t.$1:paciente_endereco_uf = 'PE' THEN 'Pernambuco'
        WHEN t.$1:paciente_endereco_uf = 'PI' THEN 'Piauí'
        WHEN t.$1:paciente_endereco_uf = 'RJ' THEN 'Rio de Janeiro'
        WHEN t.$1:paciente_endereco_uf = 'RN' THEN 'Rio Grande do Norte'
        WHEN t.$1:paciente_endereco_uf = 'RS' THEN 'Rio Grande do Sul'
        WHEN t.$1:paciente_endereco_uf = 'RO' THEN 'Rondônia'
        WHEN t.$1:paciente_endereco_uf = 'RR' THEN 'Roraima'
        WHEN t.$1:paciente_endereco_uf = 'SC' THEN 'Santa Catarina'
        WHEN t.$1:paciente_endereco_uf = 'SP' THEN 'São Paulo'
        WHEN t.$1:paciente_endereco_uf = 'SE' THEN 'Sergipe'
        WHEN t.$1:paciente_endereco_uf = 'TO' THEN 'Tocantins'
    END::varchar(100) AS patient_state_name,
    t.$1:paciente_endereco_coIbgeMunicipio::bigint AS patient_city_ibge_code,
    INITCAP(t.$1:paciente_endereco_nmMunicipio)::varchar(100) AS patient_city_name,
    t.$1:paciente_endereco_cep::int AS patient_cep_address,

    t.$1:paciente_id::varchar(100) AS patient_id,
    t.$1:paciente_idade::int AS patient_age,
    t.$1:paciente_dataNascimento::date AS patient_birth_date,
    CASE 
        WHEN t.$1:paciente_enumSexoBiologico = 'M' THEN 'Masculino'
        WHEN t.$1:paciente_enumSexoBiologico = 'F' THEN 'Feminino'
    END::varchar(15) AS patient_gender,
    INITCAP(t.$1:paciente_racaCor_valor)::varchar(30) AS patient_race,

    t.$1:vacina_dataAplicacao::date AS vaccine_administration_date,
    INITCAP(t.$1:vacina_nome)::varchar(100) AS vaccine_name,
    t.$1:vacina_descricao_dose::varchar(30) AS vaccine_dose_description,
    t.$1:vacina_lote::varchar(30) AS vaccine_batch,
    t.$1:vacina_fabricante_referencia::varchar(100) AS vaccine_manufacturer_cnpj,
    INITCAP(t.$1:vacina_fabricante_nome)::varchar(100) AS vaccine_manufacturer_name,
    INITCAP(t.$1:vacina_grupoAtendimento_nome)::varchar(100) AS patient_care_group,
    INITCAP(t.$1:vacina_categoria_nome)::varchar(100) AS vaccine_category_name,

    t.$1:sistema_origem::varchar(100) AS source_system
FROM @raw.gcs_covid/immunization/full/immunization
(FILE_FORMAT => raw.parquet_format) t
WHERE t.$1:status = 'final'
)

SELECT *
FROM staged_data