version 1.0

workflow commonVariantFilter {
    input {
        # We first need to provide arrays of the bed/bim/fam files since we are using plink1.9 formatted files
        Array[File] source_bed_files 
        Array[File] source_bim_files
        Array[File] source_fam_files
        # Then we are going to provide an value for the output plink files
        String plinkOutputPrefix = "test"
        # The goal with this workflow is to filter down the variants to a more manageable size. This will be done with a MAF filter
        Float maf
        # We can just use the standard plink docker image for most things in this workflow
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        # We are also going to subset the file to a smaller number of individuals (n=5,000). This sampling will help with runtime memory
        File? sampleFile
        # This is just the array containing the chromosome numbers from the file arrays. This is an attribute in the Terra GUI
        Array[String] chroms
        String? project_id
        # We have to specify an output bucket if we want to store the output in a folder
        String? target_gcp_folder
    }

    # This process can be parallelized over all the chromosomes
    scatter (indx in range(length(chroms))) {

        String chromosome = chroms[indx]

        File bed_file = source_bed_files[indx]
        File bim_file = source_bim_files[indx]
        File fam_file = source_fam_files[indx]

        if (defined()) {

        }
        if 
        call PlinkFilter {
            input: 
            sourceBed=bed_file,
            sourceBim=bim_file,
            sourceFam=fam_file,
            keepFile=sampleFile, 
            outputPrefix="common_variant_filter", 
            maf=maf, 
            chromosome=chromosome, 
            docker=docker,
        }

    } 

    call MergePlinkFiles { input: bed_files=PlinkFilter.output_bed, bim_files=PlinkFilter.output_bim, fam_files=PlinkFilter.output_fam, target_prefix=plinkOutputPrefix}

    if(defined(target_gcp_folder)){
        call MoveOrCopyFourFiles {
            input:
                source_file1 = MergePlinkFiles.output_merged_bed,
                source_file2 = MergePlinkFiles.output_merged_bim,
                source_file3 = MergePlinkFiles.output_merged_fam,
                source_file4 = MergePlinkFiles.output_merged_log,
                is_move_file = false,
                project_id = project_id,
                target_gcp_folder = select_first([target_gcp_folder])
            }
        }
}
