-- limpieza de datos

SELECT * FROM world_layoffs.layoffs;

-- 1. Remover Duplicados
-- 2. estandarizar los datos
-- 3. valores en null o en blanco
-- 4. borrar cualquier columna

-- crearemos una tabla de emegergencia en caso de que tengamos un error en la limpieza de datos
CREATE TABLE layoffs_staging
like layoffs;
-- esto lo que hace es solo insertar las columnas y el tipo de dato

-- ahora aca abajo insertaremos los datos de layoffs a layoffs_staging
insert layoffs_staging select * from layoffs;
-- corroboramos
SELECT * FROM world_layoffs.layoffs_staging;


-- eliminar duplicados
-- con esto agrupamos los que tengan su primera aparicion gracias al row_number que es una columna agregada
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF, date,STAGE,COUNTRY,funds_raised_millions ) AS ROW_NUM
FROM LAYOFFS_STAGING;

WITH DUPLICATE_CTE AS
(
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF, date,STAGE,COUNTRY,funds_raised_millions ) AS ROW_NUM
FROM LAYOFFS_STAGING
)
-- corroboramos cuales son los valores duplicados
SELECT *
FROM DUPLICATE_CTE
WHERE ROW_NUM > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF, date,STAGE,COUNTRY,funds_raised_millions ) AS ROW_NUM
FROM LAYOFFS_STAGING;

-- AHORA LOS BORRAMOS EN LA NUEVA TABLA

DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- ESTADANDARIZAR DATOS
-- Usamos la funcion de TRIM para eliminar los espacios del inicio y final de la cadena
UPDATE layoffs_staging2
set company = TRIM(COMPANY);

-- notamos que la industria crypto habian de 3 tipos con distintos nombres refiriendose al mismo grupo
select * from layoffs_staging2 where industry LIKE 'Crypto%';

-- ahora actualizamos los datos para que todas las semejanzas solo se llamen 'Crypto'
UPDATE layoffs_staging2
SET INDUSTRY = 'Crypto'
WHERE INDUSTRY LIKE 'Crypto%';
-- chequiamos location
SELECT DISTINCT LOCATION FROM layoffs_staging2 order by 1;
/*
nombres que falta estandarizar en LOCATION
'FlorianÃ³polis'
'DÃ¼sseldorf'
'MalmÃ¶'
*/

-- Actualizaremos estos nombres mal digitados a los nombres que realmente querian referir, debe deberse a problema con el formato 
UPDATE layoffs_staging2
SET LOCATION = CASE 
    WHEN LOCATION = 'FlorianÃ³polis' THEN 'Florianópolis'
    WHEN LOCATION = 'DÃ¼sseldorf' THEN 'Düsseldorf'
    WHEN LOCATION = 'MalmÃ¶' THEN 'Malmö'
    WHEN LOCATION = '' THEN 'N/A'
    WHEN LOCATION = 'null' THEN 'N/A'
    ELSE LOCATION  -- Mantiene el valor original si no coincide con ninguno
END;
-- revicemos los paises
SELECT DISTINCT COUNTRY
FROM layoffs_staging2
ORDER BY 1;

-- podemos ver que 'United States' tiene otra variable con punto al final 

SELECT distinct COUNTRY
FROM layoffs_staging2
where country like 'United States%';

-- Con esto actualizamos las filas 'United States.' a sin punto
-- La opción TRAILING indica que solo se eliminarán los puntos que estén al final de la cadena. No afectará a los puntos que puedan estar al principio o en el medio de la cadena.
UPDATE layoffs_staging2 
SET COUNTRY = trim(trailing '.' from country)
WHERE COUNTRY LIKE 'United States%';

-- queremos ver date ya que DATE esta puesto como texto y queremos darle el formato de fecha, ojo el tipo de dato
select `date` , STR_TO_DATE(`DATE` , '%m/%d/%y')
FROM layoffs_staging2;

-- con esto le actualizamos el formato 
-- Si los datos tienen el año en 4 dígitos (YYYY), se debe usar %Y para que MySQL lo reconozca correctamente si es de 2 digitos entonces con %y minuscula.
UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date` , '%m/%d/%Y');
/*
podemos ver que date sigue siendo tipo texto y esto es por las siguientes razones
- UPDATE solo cambia los valores de las filas, no la estructura de la tabla.
- Si la columna date ya es TEXT o VARCHAR, el valor convertido sigue almacenándose como texto.
- STR_TO_DATE() devuelve un DATE, pero MySQL no cambia automáticamente el tipo de la columna.
*/
-- con alter table si podemos modificar correctamente el tipo de dato , pero siempre hacerlo en tablas secundarias en caso de errores
ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;


-- siguiendo estandarizando los datos nos damos cuenta que igual mente hay muchas industrias null o vacias

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-- vemos que airbnb ya sabemos la industria por otro registro que tiene , por otro lado igual podemos buscar la empresa e insertale una industria
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- procedemos hacer un self join para indentificar empresas con el mismo nombre en la cual en t1 esten vacias o nulas mientras que en la 2 no esten nulas 
-- de esta forma podemos vizualizar como podemos completar industry con informacion existente de la misma tabla
select * 
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is null or t1.industry = ''
and t2.industry is not null;

-- una vez corroborada la informacion la actualizaremos la actualizaremos para que los datos de industria de t2 sean iguales a t1 bajo las condiciones 

UPDATE layoffs_staging2 t1 
join layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2. industry
where t1.industry is null or t1.industry = ''
and t2.industry is not null;

-- La primera consulta no funcionó porque la combinación de OR y AND en el WHERE no trató correctamente las filas con industry = '' (vacías).
-- por lo que opto por convertir las filas de industry vacias a null como corresponde
UPDATE layoffs_staging2
set industry = null
WHERE industry = '';

-- ahora probemos de nuevo la query especificando los nulls
UPDATE layoffs_staging2 t1 
join layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2. industry
where t1.industry is null
and t2.industry is not null;

-- ahora vemos el resultado
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'juul%';
-- y vemos que las empresas ahora ya tienen una industria 


-- analizando por otro lado la tabla notamos lo siguiente
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;
/*
-- tomaremos la importante decision de borrar varias filas que no cumplen la funcion principal de la tabla ,que seria el total de de despidos con el porcentaje de este
-- es una decision que debe ser tomada premeditadamente pero dado el caso de nuestro objetivo queremos la informacion lo mas limpia posible y trabajar con datos utiles para nuestra meta
*/

DELETE
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

-- por ahora se iria viendo nuestra tabla 
select *
from layoffs_staging2;

-- ya no nos sirve mas la columna de row_num por lo que nos desaremos de ella
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


