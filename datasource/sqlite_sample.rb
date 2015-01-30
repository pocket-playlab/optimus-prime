require 'sequel'

DB = Sequel.sqlite # memory database

DB.create_table :items do
  primary_key :id
  String :name
  Float :price
end

items = DB[:items] # Create a dataset

items.insert(:name => 'item1', :price => 100 * 100)
items.insert(:name => 'item2', :price => rand * 100)
items.insert(:name => 'item3', :price => rand * 100)
items.insert(:name => 'item4', :price => rand * 100)
items.insert(:name => 'item5', :price => rand * 100)
items.insert(:name => 'item6', :price => rand * 100)
items.insert(:name => 'item7', :price => rand * 100)
items.insert(:name => 'item8', :price => rand * 100)
items.insert(:name => 'item9', :price => rand * 100)