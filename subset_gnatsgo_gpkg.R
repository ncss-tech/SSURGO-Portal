

library(dm)


# load database connections ----
fp <- "D:/geodata/soils/gNATSGO_02_03_2025_gpkg"
dsn      <- file.path(fp, "gNATSGO_02_03_2025.gpkg")
con      <- DBI::dbConnect(RSQLite::SQLite(), dsn)
con_gpkg <- gpkg::gpkg_connect(dsn)

dsn_copy <- file.path(fp, "gNATSGO_02_03_2025 - Copy.gpkg")
con_copy <- DBI::dbConnect(RSQLite::SQLite(), dsn_copy)
con_copy_gpkg  <- gpkg::gpkg_connect(dsn_copy)


gnatsgo_dm <- dm_from_con(con)
vars <- lapply(gnatsgo_dm, colnames)


# copy existing gNATSGO database schema template to an empty geopackage ----
gNATSGO_tables   <- gpkg::gpkg_query(con_gpkg, "SELECT * FROM sqlite_master WHERE type='table';") 
gNATSGO_indices  <- gpkg::gpkg_query(con_gpkg, "SELECT * FROM sqlite_master WHERE type='index';") |>
  subset(!is.na(sql))
gNATSGO_triggers <- gpkg::gpkg_query(con_gpkg, "SELECT * FROM sqlite_master WHERE type='trigger';") |>
  subset(!is.na(sql))


# build the template ----
new_con <- gpkg::gpkg("gnatsgo_template_db_2.gpkg")
# new_con <- gpkg::gpkg_connect("gnatsgo_template_db.gpkg")
lapply(gNATSGO_tables$sql,   function(x) gpkg::gpkg_execute(new_con, x))
lapply(gNATSGO_indices$sql,  function(x) gpkg::gpkg_execute(new_con, x))
lapply(gNATSGO_triggers$sql, function(x) gpkg::gpkg_execute(new_con, x))


# set database keys ----
gnatsgo_dm <- gnatsgo_dm |>
  ## primary keys ----
  ### horizon tables ----
  dm_add_pk(chaashto,       columns = "chaashtokey") |>
  dm_add_pk(chconsistence,  columns = "chconsistkey") |>
  dm_add_pk(chdesgnsuffix,  columns = "chdesgnsfxkey") |>
  dm_add_pk(chfrags,        columns = "chfragskey") |>
  dm_add_pk(chorizon,       columns = "chkey") |>
  dm_add_pk(chpores,        columns = "chporeskey") |> 
  dm_add_pk(chstruct,       columns = "chstructkey") |>
  dm_add_pk(chstructgrp,    columns = "chstructgrpkey") |>
  dm_add_pk(chtext,         columns = "chtextkey") |>
  dm_add_pk(chtexture,      columns = "chtkey") |>
  dm_add_pk(chtexturegrp,   columns = "chtgkey") |>
  dm_add_pk(chtexturemod,   columns = "chtexmodkey") |>
  dm_add_pk(chunified,      columns = "chunifiedkey") |>
  ### component tables ----
  dm_add_pk(cocanopycover,  columns = "cocanopycovkey") |>
  dm_add_pk(cocropyld,      columns = "cocropyldkey") |>
  dm_add_pk(codiagfeatures, columns = "codiagfeatkey") |>
  dm_add_pk(coecoclass,     columns = "coecoclasskey") |>
  dm_add_pk(coeplants,      columns = "coeplantskey") |>
  dm_add_pk(coerosionacc,   columns = "coeroacckey") |>
  dm_add_pk(coforprod,      columns = "cofprodkey") |>
  dm_add_pk(coforprodo,     columns = "cofprodokey") |>
  dm_add_pk(cogeomordesc,   columns = "cogeomdkey") |>
  dm_add_pk(cohydriccriteria, columns = "cohydcritkey") |>
  dm_add_pk(cointerp,       columns = "cointerpkey") |>
  dm_add_pk(comonth,        columns = "comonthkey") |>
  dm_add_pk(component,      columns = "cokey") |>
  dm_add_pk(copm,           columns = "copmkey") |>
  dm_add_pk(copmgrp,        columns = "copmgrpkey") |>
  dm_add_pk(copwindbreak,   columns = "copwindbreakkey") |>
  dm_add_pk(corestrictions, columns = "corestrictkey") |>
  dm_add_pk(cosoilmoist,    columns = "cosoilmoistkey") |>
  dm_add_pk(cosoiltemp,     columns = "cosoiltempkey") |>
  dm_add_pk(cosurffrags,    columns = "cosurffragskey") |>
  dm_add_pk(cosurfmorphgc,  columns = "cosurfmorgckey") |>
  dm_add_pk(cosurfmorphhpp, columns = "cosurfmorhppkey") |>
  dm_add_pk(cosurfmorphmr,  columns = "cosurfmormrkey") |>
  dm_add_pk(cosurfmorphss,  columns = "cosurfmorsskey") |>
  dm_add_pk(cotaxfmmin,     columns = "cotaxfmminkey") |>
  dm_add_pk(cotaxmoistcl,   columns = "cotaxmckey") |>
  dm_add_pk(cotext,         columns = "cotextkey") |>
  dm_add_pk(cotreestomng,   columns = "cotreestomngkey") |>
  dm_add_pk(cotxfmother,    columns = "cotaxfokey") |>
  ### distribution metadata tables ----
  dm_add_pk(distinterpmd,   columns = "distinterpmdkey") |>
  dm_add_pk(distlegendmd,   columns = "distlegendmdkey") |>
  dm_add_pk(distmd,         columns = "distmdkey") |>
  # "gpkg_contents", "gpkg_extensions", "gpkg_geometry_columns", 
  # "gpkg_spatial_ref_sys", "gpkg_tile_matrix", "gpkg_tile_matrix_set",
  ### legend tables
  dm_add_pk(laoverlap,      columns = "lareaovkey") |>
  dm_add_pk(legend,         columns = "lkey") |>
  dm_add_pk(legendtext,     columns = "legtextkey") |>
  ### mapunit tables ----
  dm_add_pk(mapunit,        columns = "mukey") |>
  dm_add_pk(mutext,         columns = "mutextkey") |>
  # dm_add_pk(mdstatdomdet,   columns = "") |>
  # dm_add_pk(mdstatdommas,   columns = "") |>
  # dm_add_pk(mdstatidxdet,   columns = "") |>
  # dm_add_pk(mdstatidxmas,   columns = "") |>
  # dm_add_pk(mdstatrshipdet, columns = "") |>
  # dm_add_pk(mdstatrshipmas, columns = "") |>
  # dm_add_pk(mdstattabcols,  columns = "") |> 
  # dm_add_pk(mdstattabs,     columns = "") |>
  # dm_add_pk(month,          columns = "") |>
  dm_add_pk(muaggatt,       columns = "mukey") |>
  dm_add_pk(muaoverlap,     columns = "muareaovkey") |>
  dm_add_pk(mucropyld,      columns = "mucrpyldkey") |>
  dm_add_pk(mupolygon,      columns = "objectid") |>
  dm_add_pk(sapolygon,      columns = "objectid") |>
  dm_add_pk(mupoint,        columns = "objectid") |>
  dm_add_pk(muline,         columns = "objectid") |>
  dm_add_pk(featdesc,       columns = "featkey") |>
  dm_add_pk(featpoint,      columns = "objectid") |>
  dm_add_pk(featline,       columns = "objectid") |>
  dm_add_pk(sacatalog,      columns = "sacatalogkey") |>
  dm_add_pk(sacatalogmbr,   columns = "sacatalogmbrkey") |>
  dm_add_pk(sainterp,       columns = "sainterpkey") |>
  dm_add_pk(sdvattribute,   columns = "attributekey") |>
  dm_add_pk(sdvfolder,      columns = "folderkey") |>
  # "sdvfolderattribute", "sdvalgorithm"
  
  
  ## foreign keys ----
  ### horizon tables ----
  dm_add_fk(chorizon,              columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(chaashto,              columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chconsistence,         columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chdesgnsuffix,         columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chfrags,               columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chpores,               columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |> 
  ## structure tables
  dm_add_fk(chstructgrp,           columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chstruct,              columns     = "chstructgrpkey",
            ref_table = chstructgrp, ref_columns = "chstructgrpkey") |>
  
  dm_add_fk(chtext,                columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  ## texture
  dm_add_fk(chtexturegrp,          columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  dm_add_fk(chtexture,             columns     = "chtgkey",
            ref_table = chtexturegrp, ref_columns = "chtgkey") |>
  dm_add_fk(chtexturemod,          columns     = "chtkey",
            ref_table = chtexture, ref_columns = "chtkey") |>
  
  dm_add_fk(chunified,             columns     = "chkey",
            ref_table = chorizon,  ref_columns = "chkey") |>
  ### component tables ----
  dm_add_fk(component,             columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(cocanopycover,         columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cocropyld,             columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(codiagfeatures,        columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(coecoclass,            columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(coeplants,             columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(coerosionacc,          columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(coforprod,             columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(coforprodo,            columns     = "cofprodkey",
            ref_table = coforprod, ref_columns = "cofprodkey") |>
  ## geomorphic tables ----
  dm_add_fk(cogeomordesc,          columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cosurfmorphgc,            columns     = "cogeomdkey",
            ref_table = cogeomordesc, ref_columns = "cogeomdkey") |>
  dm_add_fk(cosurfmorphhpp,           columns     = "cogeomdkey",
            ref_table = cogeomordesc, ref_columns = "cogeomdkey") |>
  dm_add_fk(cosurfmorphmr,            columns     = "cogeomdkey",
            ref_table = cogeomordesc, ref_columns = "cogeomdkey") |>
  dm_add_fk(cosurfmorphss,            columns     = "cogeomdkey",
            ref_table = cogeomordesc, ref_columns = "cogeomdkey") |>
  
  dm_add_fk(cohydriccriteria,      columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cointerp,              columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  ### month tables ----
  dm_add_fk(comonth,               columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cosoilmoist,           columns     = "comonthkey",
            ref_table = comonth,   ref_columns = "comonthkey") |>
  dm_add_fk(cosoiltemp,            columns     = "comonthkey",
            ref_table = comonth,  ref_columns = "comonthkey") |>

  ## parent material tables
  dm_add_fk(copmgrp,               columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(copm,                  columns     = "copmgrpkey",
            ref_table = copmgrp,   ref_columns = "copmgrpkey") |>
  
  dm_add_fk(copwindbreak,          columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(corestrictions,        columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cosurffrags,           columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cotaxfmmin,            columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cotaxmoistcl,          columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cotext,                columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cotreestomng,          columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  dm_add_fk(cotxfmother,           columns     = "cokey",
            ref_table = component, ref_columns = "cokey") |>
  ### distribution metadata tables ----
  dm_add_fk(distlegendmd,          columns     = "lkey",
            ref_table = legend,    ref_columns = "lkey") |>
  dm_add_fk(distlegendmd,          columns     = "distmdkey",
            ref_table = distmd,    ref_columns = "distmdkey") |>
  dm_add_fk(distinterpmd,          columns     = "distmdkey",
            ref_table = distmd,    ref_columns  = "distmdkey") |>
  # "gpkg_contents", "gpkg_extensions", "gpkg_geometry_columns", 
  # "gpkg_spatial_ref_sys", "gpkg_tile_matrix", "gpkg_tile_matrix_set",
  
  ### legend tables ----
  # dm_add_fk(legend,                columns = "lkey") |>
  dm_add_fk(laoverlap,             columns     = "lkey",
            ref_table = legend,   ref_columns = "lkey") |>
  dm_add_fk(legendtext,            columns     = "lkey",
            ref_table = legend,    ref_columns = "lkey") |>
  
  ### mapunit tables ----
  dm_add_fk(mapunit,               columns     = "lkey",
            ref_table = legend,    ref_columns = "lkey") |>
  dm_add_fk(mutext,                columns = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  # dm_add_pk(mdstatdomdet,          columns = "") |>
  # dm_add_pk(mdstatdommas,          columns = "") |>
  # dm_add_pk(mdstatidxdet,          columns = "") |>
  # dm_add_pk(mdstatidxmas,          columns = "") |>
  # dm_add_pk(mdstatrshipdet,        columns = "") |>
  # dm_add_pk(mdstatrshipmas,        columns = "") |>
  # dm_add_pk(mdstattabcols,         columns = "") |> 
  # dm_add_pk(mdstattabs,            columns = "") |>
  dm_add_fk(muaggatt,              columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(muaoverlap,            columns     = "muareaovkey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(mucropyld,             columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  ### spatial objects ----
  dm_add_fk(mupolygon,             columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(sapolygon,             columns     = "lkey",
            ref_table = legend,    ref_columns = "lkey") |>
  dm_add_fk(mupoint,               columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(muline,                columns     = "mukey",
            ref_table = mapunit,   ref_columns = "mukey") |>
  dm_add_fk(featdesc,              columns     = "areasymbol",
            ref_table = legend,    ref_columns = "areasymbol") |>
  dm_add_fk(featpoint,             columns     = "featkey",
            ref_table = legend,    ref_columns = "areasymbol") |>
  dm_add_fk(featline,              columns     = "areasymbol",
            ref_table = legend,    ref_columns = "areasymbol") |>
  dm_add_fk(sacatalog,             columns     = "areasymbol",
            ref_table = legend,    ref_columns = "areasymbol") |>
  dm_add_fk(sacatalogmbr,          columns     = "areasymbol",
            ref_table = legend,    ref_columns = "areasymbol") |>
  dm_add_fk(sainterp,              columns     = "areasymbol",
            ref_table = legend,    ref_columns = "areasymbol")

  # dm_add_fk(sdvattribute,          columns     = "maplegendkey",
  #           ref_table = legend,    ref_columns = "lkey") |>
  # 
  # dm_add_fk(sdvfolder,                      columns     = "folderkey",
  #           ref_table = sdvfolderattribute, ref_columns = "folderkey") |>
  # dm_add_fk(sdvfolderattribute,       columns     = "attributekey",
  #           ref_table = sdvattribute, ref_columns = "attributekey")
  # "sdvalgorithm"
  


# draw database diagram ----
dm_draw(gnatsgo_dm, column_types = TRUE, rankdir = "RL")


# subset the gnatsgo_dm -----
as <- gnatsgo_dm |> 
  pull_tbl(legend) |> 
  as.data.frame() |> 
  dplyr::filter(base::grepl("^MN", areasymbol)) |> 
  select(areasymbol) |> 
  unlist() |> 
  sort() |>
  c("US")

n0 <- gnatsgo_dm |> 
  dm_nrow()

gnatsgo_dm_sub <-  gnatsgo_dm |> 
  # dm_select_tbl(legend, mapunit, component) |> 
  dm_filter(legend = (areasymbol %in% as))
n1 <- gnatsgo_dm_sub |> dm_nrow()

# compare size of the full vs sub
n1 - n0


# copy the gnatsgo_dm_sub to the template ----
new_con <- gpkg::gpkg_connect("gnatsgo_template_db.gpkg")
nm <- names(gnatsgo_dm_sub)
nm <- nm[!grepl("rtree_|gpkg_|polygon|point|line", nm)]

lapply(nm, function(x) {
  cat(as.character(Sys.time()), x, "\n")
  
  tmp <- collect(gnatsgo_dm_sub[[x]])
  gpkg::gpkg_write_attributes(x = new_con, table = tmp, table_name = x, append = TRUE)
})


