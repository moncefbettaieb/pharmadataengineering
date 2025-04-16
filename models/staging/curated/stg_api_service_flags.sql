{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['doc_id']}
    ]
  )
}}

select
    id,
    doc->>'_id' as doc_id,
    -- Flags principaux
    (doc->'properties'->'flags'->>'homeo')::integer as is_homeo,
    (doc->'properties'->'flags'->>'doping')::integer as is_doping,
    (doc->'properties'->'flags'->>'generic')::integer as is_generic,
    (doc->'properties'->'flags'->>'internal')::integer as is_internal,
    (doc->'properties'->'flags'->>'narcotic')::integer as is_narcotic,
    (doc->'properties'->'flags'->>'referent')::integer as is_referent,
    (doc->'properties'->'flags'->>'crushable')::integer as is_crushable,
    (doc->'properties'->'flags'->>'biosimilar')::integer as is_biosimilar,
    (doc->'properties'->'flags'->>'pediatrics')::integer as is_pediatrics,
    (doc->'properties'->'flags'->>'hybrid')::integer as hybrid,
    doc->'properties'->'flags'->>'driveWarning' as drive_warning,
    doc->'properties'->'flags'->>'medicalDevice' as medicalDevice,
    doc->'properties'->'flags'->>'hospitalDelivery' as hospitalDelivery,
    doc->'properties'->'flags'->>'exactcureSimulable' as exactcureSimulable,
    -- Flags étendus
    (doc->'properties'->'flags'->'extendedFlags'->>'ghs')::integer as is_ghs,
    (doc->'properties'->'flags'->'extendedFlags'->>'vaccin')::integer as is_vaccin,
    (doc->'properties'->'flags'->'extendedFlags'->>'fridge')::integer as needs_fridge,
    (doc->'properties'->'flags'->'extendedFlags'->>'freezer')::integer as needs_freezer,
    (doc->'properties'->'flags'->'extendedFlags'->>'mte')::integer as mte,
    (doc->'properties'->'flags'->'extendedFlags'->>'t2a')::integer as t2a,
    (doc->'properties'->'flags'->'extendedFlags'->>'tfr')::integer as tfr,
    (doc->'properties'->'flags'->'extendedFlags'->>'horsGhs')::integer as horsGhs,
    (doc->'properties'->'flags'->'extendedFlags'->>'testReagent')::integer as testReagent,
    (doc->'properties'->'flags'->'extendedFlags'->>'abortionDrug')::integer as abortionDrug,
    (doc->'properties'->'flags'->'extendedFlags'->>'firstAgeMilk')::integer as firstAgeMilk,
    (doc->'properties'->'flags'->'extendedFlags'->>'anticoagulant')::integer as anticoagulant,
    (doc->'properties'->'flags'->'extendedFlags'->>'drugException')::integer as drugException,
    (doc->'properties'->'flags'->'extendedFlags'->>'hybridReferent')::integer as hybridReferent,
    (doc->'properties'->'flags'->'extendedFlags'->>'soluteDilution')::integer as soluteDilution,
    (doc->'properties'->'flags'->'extendedFlags'->>'bloodDerivative')::integer as bloodDerivative,
    (doc->'properties'->'flags'->'extendedFlags'->>'deconditionning')::integer as deconditionning,
    (doc->'properties'->'flags'->'extendedFlags'->>'humanAntibiotic')::integer as humanAntibiotic,
    (doc->'properties'->'flags'->'extendedFlags'->>'communityApproval')::integer as communityApproval,
    (doc->'properties'->'flags'->'extendedFlags'->>'biosimilarReferent')::integer as biosimilarReferent,
    (doc->'properties'->'flags'->'extendedFlags'->>'minorContraceptive')::integer as minorContraceptive,
    (doc->'properties'->'flags'->'extendedFlags'->>'pregnancyPictogram')::integer as pregnancyPictogram,
    (doc->'properties'->'flags'->'extendedFlags'->>'seasonalFluVaccine')::integer as seasonalFluVaccine,
    (doc->'properties'->'flags'->'extendedFlags'->>'infertilityTreatment')::integer as infertilityTreatment,
    (doc->'properties'->'flags'->'extendedFlags'->>'reinforcedMonitoring')::integer as reinforcedMonitoring,
    (doc->'properties'->'flags'->'extendedFlags'->>'veterinaryAntibiotic')::integer as veterinaryAntibiotic,
    (doc->'properties'->'flags'->'extendedFlags'->>'mindalteringSubstance')::integer as mindalteringSubstance,
    (doc->'properties'->'flags'->'extendedFlags'->>'emergencyContraceptive')::integer as emergencyContraceptive,
    (doc->'properties'->'flags'->'extendedFlags'->>'veterinaryPrescription')::integer as veterinaryPrescription
from {{ source('pharma_sources', 'mycollection') }} 