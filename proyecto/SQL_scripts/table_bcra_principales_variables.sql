-- aisnardi_coderhouse.bcra_principales_variables definition

-- Drop table

-- DROP TABLE aisnardi_coderhouse.bcra_principales_variables;

--DROP TABLE aisnardi_coderhouse.bcra_principales_variables;
CREATE TABLE IF NOT EXISTS aisnardi_coderhouse.bcra_principales_variables
(
	idvariable INTEGER NOT NULL  ENCODE az64
	,cdserie INTEGER   ENCODE az64
	,descripcion VARCHAR(255)   ENCODE lzo
	,fecha DATE NOT NULL  ENCODE az64
	,valor DOUBLE PRECISION   ENCODE RAW
	,createddate TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
	,PRIMARY KEY (idvariable, fecha)
)
DISTSTYLE AUTO
;
ALTER TABLE aisnardi_coderhouse.bcra_principales_variables owner to aisnardi_coderhouse;