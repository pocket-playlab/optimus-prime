Sequel.migration do
  up do
    create_table(:load_jobs) do
      primary_key :id
      String :identifier
      String :job_id
      String :operation_id, null: false, index: true
      String :uris
      String :category
      String :status, null: false
      Timestamp :start_time
      Timestamp :end_time
      String :error
    end
  end

  down do
    drop_table(:load_jobs)
  end
end
