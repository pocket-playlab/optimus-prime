Sequel.migration do
  up do
    create_table(:operations) do
      primary_key :id
      String :pipeline_id, null: false, index: true
      Timestamp :start_time
      Timestamp :end_time
      String :status, null: false
      Text :error
    end
  end

  down do
    drop_table(:operations)
  end
end
