--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
---                                                                                ---
---                             FOR REFERENCE ONLY                                 ---
---                                                                                ---
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------


-- 1) Core reference tables

CREATE TABLE Stimuli (
  id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  iri              VARCHAR(255)      NOT NULL UNIQUE,  
  label            VARCHAR(120)      NOT NULL,          
  class_hint       VARCHAR(40)       NULL,              

  -- Research-centric metadata:
  chem_formula     VARCHAR(100)      NULL COMMENT 'Molecular formula (e.g. C4H8O2)',
  smiles           VARCHAR(255)      NULL COMMENT 'SMILES representation',
  molecular_weight DECIMAL(8,3)      NULL COMMENT 'g/mol',
  default_dose     DECIMAL(10,4)     NULL COMMENT 'Default concentration',
  dose_unit        VARCHAR(20)       NULL COMMENT 'e.g. mM, µg/mL',
  
  KEY idx_stimuli_label (label)
);

CREATE TABLE Taxa (
  id             INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  iri            VARCHAR(255)      NOT NULL UNIQUE,
  species_name   VARCHAR(120)      NOT NULL,        
  kingdom        ENUM('Bacteria','Eukaryota','Archaea') NULL,
  ranking        VARCHAR(30)       NULL,             

  -- Research-centric metadata:
  gc_content     DECIMAL(5,2)      NULL COMMENT 'Percent GC in genome',
  genome_length  BIGINT UNSIGNED   NULL COMMENT 'bp total',
  habitat         VARCHAR(80)      NULL COMMENT 'Natural habitat',
  pathogenicity   ENUM('commensal','opportunist','pathogen') NULL,

  KEY idx_taxa_kingdom (kingdom),
  KEY idx_taxa_species (species_name)
);

CREATE TABLE OntologyTerms (
  id        INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  iri       VARCHAR(255)      NOT NULL UNIQUE,
  label     VARCHAR(120)      NOT NULL,
  ontology  VARCHAR(80)       NOT NULL,            

  -- Research-centric metadata:
  term_definition TEXT             NULL COMMENT 'Full term definition',
  synonyms   TEXT             NULL COMMENT 'Pipe-separated synonyms',
  onto_version    VARCHAR(20)      NULL COMMENT 'Ontology version',

  KEY idx_ontol_label (label),
  KEY idx_ontol_iri (iri),
  KEY idx_ontol_ont   (ontology)
);

CREATE TABLE Studies (
  id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  iri           VARCHAR(255)      NOT NULL UNIQUE,  
  title         VARCHAR(255)      NOT NULL,
  source_repo   ENUM('ArrayExpress','CELLxGENE') NOT NULL,

  -- Research-centric metadata:
  publication_date DATE           NULL COMMENT 'Date published',
  study_type      ENUM('transcriptomic','proteomic','multiomic') NULL,
  num_samples     INT UNSIGNED    NULL COMMENT 'Total samples in study',
  contact_email   VARCHAR(120)    NULL COMMENT 'Lead author email',
  
  KEY idx_studies_date (publication_date),
  KEY idx_studies_type (study_type)
);


-- 2) Dependent entities

CREATE TABLE Microbes (
  id                        INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  taxon_iri                  INT UNSIGNED NOT NULL,
  strain_name               VARCHAR(120)    NULL,
  culture_collection        VARCHAR(80)     NULL,
  genome_assembly_accession VARCHAR(30)     NULL,
  genome_size_bp            BIGINT UNSIGNED NULL,
  oxygen_requirement        ENUM('aerobe','anaerobe','facultative') NULL,
  habitat                   VARCHAR(80)     NULL,
  optimal_growth_temp       DECIMAL(4,1)    NULL,
  doubling_time             DECIMAL(5,2)    NULL,
  metabolic_profile_iri     VARCHAR(255)    NULL,

  -- Research-centric metadata:
  abundance_index           DECIMAL(8,4)    NULL COMMENT 'Relative abundance metric',
  prevalence                INT UNSIGNED    NULL COMMENT 'Count of studies observed',

  FOREIGN KEY (taxon_iri) REFERENCES Taxa(iri) ON DELETE RESTRICT,
  KEY idx_microbes_taxon   (taxon_iri),
  KEY idx_microbes_strain  (strain_name),
  KEY idx_microbes_habitat (habitat)
);

CREATE TABLE Genes (
  id                  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  gene_accession      VARCHAR(20)      NOT NULL, 
  gene_name           VARCHAR(120)     NOT NULL,
  species_taxon_iri    INT UNSIGNED     NOT NULL,
  -- Research-centric metadata:
  gene_length_bp      INT UNSIGNED     NULL COMMENT 'Length of gene in bp',
  gc_content          DECIMAL(5,2)     NULL COMMENT '% GC in coding region',
  pathway_iri         VARCHAR(255)     NULL COMMENT 'Link to KEGG/Reactome',
  go_terms            TEXT             NULL COMMENT 'Pipe-separated GO IDs',

  FOREIGN KEY (species_taxon_iri) REFERENCES Taxa(iri) ON DELETE RESTRICT,
  UNIQUE KEY uq_gene_acc_taxon (gene_accession, species_taxon_id),
  KEY idx_genes_acc    (gene_accession),
  KEY idx_genes_taxon  (species_taxon_id)
);


-- 3) Samples and measurements

CREATE TABLE Samples (
  id                 INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  iri                VARCHAR(255)      NOT NULL UNIQUE,   
  study_iri           INT UNSIGNED      NOT NULL,
  cell_type_iri       INT UNSIGNED      NOT NULL,
  tissue_iri          INT UNSIGNED      NOT NULL,
  organism_iri        INT UNSIGNED      NOT NULL,
  growth_condition   VARCHAR(120)      NOT NULL,          
  raw_counts_uri     VARCHAR(255)      NOT NULL,

  -- Research-centric metadata:
  collection_date    DATE             NULL COMMENT 'Date sample was taken',
  donor_age_years    INT UNSIGNED     NULL COMMENT 'Age of donor',
  replicate_number   INT UNSIGNED     NULL COMMENT 'Biological replicate #',
  viability_pct      DECIMAL(5,2)     NULL COMMENT 'Cell viability %',
  rin_score          DECIMAL(5,3)     NULL COMMENT 'RNA Integrity Number',

  FOREIGN KEY (study_iri) REFERENCES Studies(iri)       ON DELETE RESTRICT,
  FOREIGN KEY (cell_type_iri) REFERENCES OntologyTerms(iri) ON DELETE RESTRICT,
  FOREIGN KEY (tissue_iri) REFERENCES OntologyTerms(iri) ON DELETE RESTRICT,
  FOREIGN KEY (organism_iri) REFERENCES OntologyTerms(iri) ON DELETE RESTRICT,

  KEY idx_samples_study    (study_iri),
  KEY idx_samples_cell     (cell_type_iri),
  KEY idx_samples_tissue  (tissue_iri),
  KEY idx_samples_organism (organism_iri),
  KEY idx_samples_date     (collection_date)
);

CREATE TABLE ExpressionStats (
  expr_id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  sample_id      INT UNSIGNED    NOT NULL,
  gene_id        INT UNSIGNED    NOT NULL,
  log2_fc        DECIMAL(10,4)   NOT NULL,
  p_value        DECIMAL(10,4)   NOT NULL CHECK (p_value BETWEEN 0 AND 1),
  significance   ENUM('Low','Moderate','High') NOT NULL,

  -- Research-centric metadata:
  adjusted_pval  DECIMAL(10,4)   NULL COMMENT 'FDR-adjusted p-value',
  base_mean      DECIMAL(12,3)   NULL COMMENT 'Mean normalized count',
  counts          BIGINT UNSIGNED NULL COMMENT 'Raw read count',

  FOREIGN KEY (sample_id) REFERENCES Samples(id) ON DELETE RESTRICT,
  FOREIGN KEY (gene_id) REFERENCES Genes(id)   ON DELETE RESTRICT,

  KEY idx_expr_sample (sample_id),
  KEY idx_expr_gene   (gene_id)
);


-- 4) Relationship (M:N) tables

CREATE TABLE SampleMicrobe (
  id           INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  sample_id    INT UNSIGNED NOT NULL,
  microbe_id   INT UNSIGNED NOT NULL,
  evidence     ENUM('mgnify','literature','inferred') DEFAULT 'mgnify',

  -- Research-centric metadata:
  relative_abundance DECIMAL(10,6) NULL COMMENT 'Relative abundance in sample',

  FOREIGN KEY (sample_id) REFERENCES Samples(id)  ON DELETE CASCADE,
  FOREIGN KEY (microbe_id) REFERENCES Microbes(id) ON DELETE CASCADE,

  KEY idx_smp_micro_sample  (sample_id),
  KEY idx_smp_micro_microbe (microbe_id)
);

CREATE TABLE SampleStimulus (
  id           INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  sample_id    INT UNSIGNED NOT NULL,
  stimulus_id  INT UNSIGNED NOT NULL,

  -- Research-centric metadata:
  exposure_time_hr  DECIMAL(5,2) NULL COMMENT 'Duration of exposure',
  response_marker   VARCHAR(120) NULL COMMENT 'e.g. cytokine name',

  FOREIGN KEY (sample_id)
    REFERENCES Samples(id)  ON DELETE CASCADE,
  FOREIGN KEY (stimulus_id)
    REFERENCES Stimuli(id) ON DELETE CASCADE,

  KEY idx_smp_stim_sample   (sample_id),
  KEY idx_smp_stim_stimulus (stimulus_id)
);

CREATE TABLE MicrobeStimulus (
  id           INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  microbe_id   INT UNSIGNED NOT NULL,
  stimulus_id  INT UNSIGNED NOT NULL,
  evidence     ENUM('mgnify','literature','inferred') DEFAULT 'mgnify',

  -- Research-centric metadata:
  interaction_score DECIMAL(5,3) NULL COMMENT 'Scored interaction strength',

  FOREIGN KEY (microbe_id) REFERENCES Microbes(id) ON DELETE CASCADE,
  FOREIGN KEY (stimulus_id) REFERENCES Stimuli(id)  ON DELETE CASCADE,

  KEY idx_mic_stim_microbe   (microbe_id),
  KEY idx_mic_stim_stimulus  (stimulus_id)
);


-- 5) Auditing and staging

CREATE TABLE ActivityLog (
  log_id      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_name   VARCHAR(60),
  table_name  VARCHAR(60),
  action      ENUM('INSERT','UPDATE','DELETE'),
  action_ts   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging_kv (
  file_id     VARCHAR(100),     
  column_key  VARCHAR(120),
  value       TEXT,
  PRIMARY KEY (file_id, column_key)
);
