
library(terra)
library(parallel)

terraOptions(memfrac = 0.8) # Fraction of memory to allow terra

tmpdir        <- "/mnt/c/Rwork"
daily_ppt_dir <- "/mnt/g/PRISM/Daily/4km"
output_dir    <- "/mnt/g/PRISM/8-day/0.20"
year_list     <- c(2018:2020)

tmp_create <- function(tmpdir) {
  
  p_tmp_dir <- paste0(tmpdir, "/", as.character(Sys.getpid())) # Process ID
  
  if (!dir.exists(p_tmp_dir)) {
    dir.create(p_tmp_dir, recursive = TRUE)
  }
  
  terraOptions(tempdir = p_tmp_dir)
}
tmp_remove <- function(tmpdir) {
  
  p_tmp_dir <- paste0(tmpdir, "/", as.character(Sys.getpid())) # Process ID
  unlink(p_tmp_dir, recursive = TRUE)
}
check_leap <- function(year) {
  if((year %% 4) == 0) {
    if((year %% 100) == 0) {
      if((year %% 400) == 0) {
        return(TRUE)
      } else {
        return(FALSE)
      }
    } else {
      return(TRUE)
    }
  } else {
    return(FALSE)
  }
}
to_8day    <- function(year, in_dir, out_dir, tmpdir) {
  
  tmp_create(tmpdir)
  
  start <- Sys.time() # Start clock for timing
  
  print(paste0("Starting ", year, ". Start time is ", start))
  
  output_dir <- paste0(out_dir, "/", year)
  print(paste0("Output dir is: ", output_dir))
  
  template_raster <- rast(xmin = -180, xmax = 180, ymin = -90, ymax = 90,
                          ncols = 7200, nrows = 3600, crs = "+proj=longlat +datum=WGS84")
  
  if (!dir.exists(output_dir)) { # Create output dirs for each year
    dir.create(output_dir, recursive = TRUE)
  }
  
  sub_dir_files <- list.files(paste0(in_dir, "/", year), full.names = TRUE, pattern = "*.bil$")
  
  for (h in seq(1, length(sub_dir_files), 8)) {
    
    print(paste0("Running 8-day mean starting with DOY ", h))
    
    file_list_8day <- sub_dir_files[h : (h + 7)]
    file_list_8day <- file_list_8day[!is.na(file_list_8day)]
    
    if (h != 361 && length(file_list_8day) == 8) {
      
      ppt_stack <- rast(file_list_8day)
      flag     <- TRUE
      
    } else if (h == 361 && (length(sub_dir_files) == 365 || length(sub_dir_files == 366))) {
      
      ppt_stack <- rast(file_list_8day)
      flag     <- TRUE
      
    } else if (h != 361 && length(file_list_8day) != 8) {
      
      print(paste0("Skipping DOY ", h, " for year ", year, " due to insufficient number of files."))
      flag     <- FALSE
    }
    
    # Mean up the rasters and get output file name
    if (flag == TRUE) {
      
      out_name  <- substr(basename(sub_dir_files[h]), 1, 31) # Get first 16 characters of filename
      out_name  <- paste0(output_dir, "/", out_name, ".8day.tif")
      
      ppt_out   <- sum(ppt_stack, na.rm = TRUE)
      ppt_out   <- terra::project(ppt_out, template_raster)
      ppt_out   <- round(ppt_out, digits = 3) * 1000
      
      writeRaster(ppt_out, filename = out_name, overwrite = TRUE, NAflag = -9999, datatype = 'INT4S')
      print(paste0("Saved file to: ", out_name))
    }
  }
  tmp_remove(tmpdir)
}


# Dedicate each year to a core
mclapply(year_list, to_8day, mc.cores = 3, in_dir = daily_ppt_dir, out_dir = output_dir, tmpdir = tmpdir)