Sequel.migration do
  up do
    create_table(:jobs) do
      primary_key :id
      String :operation_id, null: false, index: true
      String :uri
      String :category
      String :status, null: false
      Timestamp :created_at
      Timestamp :uploaded_at
      Timestamp :imported_at
      String :error
    end
  end

  down do
    drop_table(:jobs)
  end
end
