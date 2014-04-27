require 'test_helper'

require 'elasticsearch/persistence/model'

module Elasticsearch
  module Persistence
    class PersistenceModelIntegrationTest < Elasticsearch::Test::IntegrationTestCase

      class ::Person
        include Elasticsearch::Persistence::Model

        settings index: { number_of_shards: 1 }

        attribute :name, String,
                  mapping: { fields: {
                    name: { type: 'string', analyzer: 'snowball' },
                    raw:  { type: 'string', analyzer: 'keyword' }
                  } }

        attribute :birthday,   Date
        attribute :department, String
        attribute :salary,     Integer
        attribute :admin,      Boolean, default: false

        validates :name, presence: true
      end

      context "A basic persistence model" do
        should "save the document" do
          person = Person.new name: 'John Smith', birthday: Date.parse('1970-01-01')
          person.save

          assert_not_nil person.id
          document = Person.find(person.id)

          assert_instance_of Person, document
          assert_equal 'John Smith', document.name
          assert_equal 'John Smith', Person.find(person.id).name

          assert_not_nil Elasticsearch::Persistence.client.get index: 'people', type: 'person', id: person.id
        end

        should "delete the document" do
          person = Person.create name: 'John Smith', birthday: Date.parse('1970-01-01')

          person.destroy
          assert person.frozen?

          assert_raise Elasticsearch::Transport::Transport::Errors::NotFound do
            Elasticsearch::Persistence.client.get index: 'people', type: 'person', id: person.id
          end
        end

        should "update a document attribute" do
          person = Person.create name: 'John Smith'

          person.update name: 'UPDATED'

          assert_equal 'UPDATED', person.name
          assert_equal 'UPDATED', Person.find(person.id).name
        end

        should "increment a document attribute" do
          person = Person.create name: 'John Smith', salary: 1_000

          person.increment :salary

          assert_equal 1_001, person.salary
          assert_equal 1_001, Person.find(person.id).salary
        end

        should "update the document timestamp" do
          person = Person.create name: 'John Smith', salary: 1_000
          updated_at = person.updated_at

          sleep 1
          person.touch

          assert person.updated_at > updated_at, [person.updated_at, updated_at].inspect
          assert Person.find(person.id).updated_at > updated_at, [person.updated_at, updated_at].inspect
        end
      end

    end
  end
end
