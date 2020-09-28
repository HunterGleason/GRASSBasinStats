#'Wrapper. Function reads in a table of coordinates with a unique ID 'UID' field and
#'delineates upstream basin based on a digital elevation model --assumed to
#'be present in the initialized GRASS environment--.
#'
#'@param basin_df A data frame with x-y coordinates --labeled as x & y-- of pour points to delineate and a unique identifier feild --labeld UID--.
#'@param procs Number of threads to run in parallel, recommend no more than 90% of system total.
#'@return Nothing, however initialized GRASS environment is populated with delineated basins as GRASS raster files.
#'@export
delin_basin<-function(basin_df,procs)
{
  #Generate series of GRASS comands based on information in basin_df
  cmds<-as.data.frame(paste('r.water.outlet input=dir@PERMANENT coordinates=',basin_df$x,",",basin_df$y,' output=basin_',basin_df$UID,' --overwrite --quiet',sep=""))
  cmd_tbl<-as.data.frame(cmds)
  
  #A temporary path to save GRASS commands
  cmd_pth=paste(tempdir(),'/grass_delin_basin_cmd.txt',sep="")
  
  #Write out commands to temp file 'grass_delin_basin_cmd.txt'
  write.table(cmd_tbl,file = cmd_pth,row.names = F,col.names = F,quote = F)
  
  #Set up executable '/grass_delin_basin.sh'
  script_pth=paste(tempdir(),'/grass_delin_basin.sh',sep="")
  script<-c()
  script[1]<-"#!/bin/bash"
  
  #Use the parallel command to run commands in '/grass_delin_basin_cmd.txt', save as temp '/grass_delin_basin.sh'. Set jobs to 'procs' param
  cmd_str<-paste('parallel --jobs ',procs,' < ',cmd_pth,sep="")
  script<-as.data.frame(rbind(script,cmd_str,"exit 0"))
  
  #Write our executable
  write.table(script,file = script_pth,row.names = F,col.names = F,quote = F)
  
  #Set permissions
  system(paste('chmod u+x ',script_pth,sep=""))
  
  #Call '/grass_delin_basin.sh' through GRASS
  sys_call<-paste('grass78 ',grass_Db,'/',grass_loc,'/PERMANENT/ --exec sh ',script_pth,sep="")
  
  print("Delineating basins, this may take a while ...")
  system(sys_call)
  print("Basins delineated")
}

#'Wrapper. Function reads in a table of coordinates with a unique ID 'UID' field and
#'calculates raster statistics for a input GRASS raster. Statistics are calculated
#'using GRASS r.univar and written to the ouput directory as txt files.
#'
#'@param basin_df A data frame with x-y coordinates --labeled as x & y-- of pour points to delineate and a unique identifier feild --labeld UID--.
#'@param stat_rast Name of input GRASS raster to calculate basin statistics as string, e.g., 'slope'. Assumed present in GRASS initialized environment.
#'@param procs Number of threads to run in parallel, recommend no more than 90% of system total.
#'@param out_dir Output directory for GARSS r.univar output txt files with basin statistics.
#'@return Nothing, however output directory is populated with basin statistics as txt files.
#'@export
calc_basin_stats<-function(basin_df,stat_rast,procs,out_dir)
{
  #Generate series of GRASS commands using information provided in basin_df, save output stats txt files to output directory 'out_dir'
  cmds<-as.data.frame(paste('r.univar -g --overwrite map=',stat_rast,' zones=basin_',basin_df$UID,' output=',out_dir,'/basin_stats_',basin_df$UID,'.txt separator=newline',sep=""))
  
  #Define temp path for GRASS commands and write out
  cmd_pth=paste(tempdir(),'/grass_basin_stats_cmds.txt',sep="")
  write.table(cmds,file = cmd_pth,row.names = F,col.names = F,quote = F)
  
  #Set up temp executable '/grass_basin_stats.sh'
  script_pth=paste(tempdir(),'/grass_basin_stats.sh',sep="")
  script<-c()
  script[1]<-"#!/bin/bash"
  
  #Run commands in '/grass_basin_stats_cmds.txt' using parallel command, set jobs to 'procs' param
  cmd_str<-paste('parallel --jobs ',procs,' < ',cmd_pth,sep="")
  
  #Combine as a executable script and write to temp '/grass_basin_stats.sh'
  script<-as.data.frame(rbind(script,cmd_str,"exit 0"))
  write.table(script,file = script_pth,row.names = F,col.names = F,quote = F)
  
  #Set permissions
  system(paste('chmod u+x ',script_pth,sep=""))
  
  #Call the '/grass_basin_stats.sh' executable through GRASS
  sys_call<-paste('grass78 ',grass_Db,'/',grass_loc,'/PERMANENT/ --exec sh ',script_pth,sep="")
  
  print("Calculating basins stats, this may take a while ...")
  system(sys_call)
  print("Calculated basins stats.")
}

#'Function reads in a table of coordinates with a unique ID 'UID' field and
#'for each UID open the corresponding output basin statistics txt file from
#''calc_basin_stats' and reads data to data.frame.
#'
#'@param basin_df A data frame with x-y coordinates --labeled as x & y-- of pour points to delineate and a unique identifier feild --labeld UID--.
#'@param stat_dir Path to output directory with r.univar statistic txt files from 'calc_basin_stats' as character vector.
#'@return Data.frame with UID field and corresponding upstream statistics.
#'@export
stats_to_df<-function(basin_df,stat_dir)
{
  
  #Define global vars
  rslt<-c()
  
  #For each row in basin_df
  for(r in c(1:nrow(basin_df)))
  {
    #Get the stats for basin by UID from txt file as data.frame
    stats<-readr::read_lines(paste(stat_dir,'/basin_stats_',basin_df$UID[r],'.txt',sep=""))
    
    #Get user specified statistic ('stat') from stats data.frame, assumed format. 
    #N
    N<-as.numeric(strsplit(stats[2],"=")[[1]][2])
    #NULL_CELLS
    NULL_CELLS<-as.numeric(strsplit(stats[3],"=")[[1]][2])
    #CELLS
    CELLS<-as.numeric(strsplit(stats[4],"=")[[1]][2])
    #MIN
    MIN<-as.numeric(strsplit(stats[5],"=")[[1]][2])
    #MAX
    MAX<-as.numeric(strsplit(stats[6],"=")[[1]][2])
    #RANGE
    RANGE<-as.numeric(strsplit(stats[7],"=")[[1]][2])
    #MEAN
    MEAN<-as.numeric(strsplit(stats[8],"=")[[1]][2])
    #MAE
    MAE<-as.numeric(strsplit(stats[9],"=")[[1]][2])
    #STDDEV
    STDDEV<-as.numeric(strsplit(stats[10],"=")[[1]][2])
    #VAR
    VAR<-as.numeric(strsplit(stats[11],"=")[[1]][2])
    #SUM
    SUM<-as.numeric(strsplit(stats[13],"=")[[1]][2])
    
    stat_vec<-c(basin_df$UID[r],N,NULL_CELLS,CELLS,MIN,MAX,RANGE,MEAN,MAE,STDDEV,VAR,SUM)

    if(r==1)
    {
    	rslt<-stat_vec
    }else
    {
    	rslt<-rbind(rslt,stat_vec)
    }
    
  }
  
  #Form a result data.frame with UID and basic statistic as fields
  rslt<-as.data.frame(rslt)
  
  colnames(rslt)<-c('UID','N','NULL_CELLS','CELLS','MIN','MAX','RANGE','MEAN','MAE','STDDEV','VAR','SUM')
  
  return(rslt)
  
}

#'Main function. Reads in a table of x-y pour point coordinates with a unique ID 'UID' field and
#'delineates upstream basin area for each UID. Using the delineated basins, upstream statistics
#'are calculated for a input raster layer and returned as a data.frame. The GRASS r.water.outlet 
#'and r.univar functions are used for computation, therefore, an initialized GRASS environment with 
#'a digital elevation model must be active, as well as the layers for which statistics are derived. 
#'Note these are wrapper functions that assume that the GNU parallel package is available.
#'
#'@param basin_df A data frame with x-y coordinates --labeled as x & y-- of pour points to delineate and a unique identifier field --labeled UID--.
#'@param procs Number of threads to run in parallel, recommend no more than 90% of system total.
#'@param stat_rast Name of input GRASS raster to calculate basin statistics as string, e.g., 'slope'. Assumed present in GRASS initialized environment.
#'@return Data.frame with UID field and corresponding upstream statistics.
#'@export
get_basin_stats<-function(basin_df,procs,stat_rast)
{
  #Delineate basins from DEM for all pour points in basin_df using GRASS r.water.outlet
  delin_basin(basin_df,procs)
  
  #Create temp dir for out basin statistics txt files
  stat_dir<-paste(tempdir(),"/temp_basin_stats/",sep="")
  system(paste("mkdir ",stat_dir,sep=""))
  
  #Calculate upstream basin statistics for 'stat_rast' for each pour point, write stat txt files to 'stat_dir'
  calc_basin_stats(basin_df,stat_rast,procs,stat_dir)
  
  #Read basin stat txt files from 'stat_dir' and return specified statistic 'stat' as data.frame
  stat_vec<-stats_to_df(basin_df,stat_dir)
  
  #Clean up temp files
  rgrass7::execGRASS('g.remove',parameters = list(type='raster',pattern='basin_*'),flags=c('f'))
  system(paste("rm -r ",stat_dir,sep=""))
  
  #Return basin stat data.frame
  return(stat_vec)
}
