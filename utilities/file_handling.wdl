version 1.0

# The third task is used to copy all of the output from the plink merging step into the GCP project bucket.
# This task is adapted from the "MoveOrCopyThreeFiles" found here: https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl
task MoveOrCopyFourFiles {
    input {
        File source_file1
        File source_file2
        File source_file3
        File source_file4

        Boolean is_move_file = false

        Int memory = 5
        String? project_id
        String target_gcp_folder
    }

    String action = if (is_move_file) then "mv" else "cp"

    String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

    String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
    String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
    String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
    String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"

    command <<<

    set -e

    gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{gcs_output_dir}/

    >>>

    runtime {
        docker: "google/cloud-sdk"
        disks: "local-disk 10 SSD"
        memory: memory + "GiB"
    }
    output {
        String output_file1 = new_file1
        String output_file2 = new_file2
        String output_file3 = new_file3
        String output_file4 = new_file4
    }
}