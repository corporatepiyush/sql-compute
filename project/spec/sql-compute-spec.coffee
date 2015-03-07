describe 'Sql Compute Library', ->

  describe 'Select Clause', ->

    describe 'Validations and Exceptions', ->

      it 'should raise an exception if value is not an Array', ->
        command =
          select : 'db.firstName'

        expect(-> new sqlCompute.Select(command).validate()).toThrowError("The value of 'select' should be of type Array")

      it 'should raise an exception if db alias is incorrect', ->
        command =
          select : ['db.firstName', 'db2.lastName']
          from :
              db : sqlCompute.ArrayDataSource([1, 2, 3])

        expect(-> new sqlCompute.Select(command).validate()).toThrowError("Invalid database alias 'db2'")


  describe 'From Clause', ->

    describe 'Validations and Exceptions', ->

      it 'should raise an expection if value is not an Object', ->
        command =
          from : 'db'

        expect(-> new sqlCompute.From(command).validate()).toThrowError("The value of 'from' should be of type JSON Object")


      it 'should raise an exception provided list of datasource object does not conform', ->
        command =
          from :
            db1 : sqlCompute.ArrayDataSource([1, 2, 3])
            db2 : 'illegal'

        expect(-> new sqlCompute.From(command).validate()).toThrowError("The value of 'datasource db2' should be of type either ArrayDataSource or CustomDataSource")


  describe 'Where Clause', ->

    it 'should raise an exception if its not a function', ->
      command =
        where : 'i am not function'

      expect(-> new sqlCompute.Where(command).validate()).toThrowError("The value of 'where' should be of type Function")


  describe 'GroupBy Clause', ->

    it 'should raise an exception if value is not an Array ', ->
      command =
        groupBy : { not : 'an array' }

      expect(-> new sqlCompute.GroupBy(command).validate()).toThrowError("The value of 'groupBy' should be of type Array")


  describe 'Having Clause', ->

    it 'should raise an exception if groupBy is not mentioned', ->
      command =
        having : { condition : -> }

      expect(-> new sqlCompute.Having(command).validate()).toThrowError("'groupBy' is required if 'having' is used")

    it 'should raise an exception if condition is not mentioned', ->
      command =
        groupBy : ['a.a','a.b']
        having : { }

      expect(-> new sqlCompute.Having(command).validate()).toThrowError("'having.condition' is required if 'having' is used")

    it 'should raise an exception if condition is not a function', ->
      command =
        groupBy : ['a.a','a.b']
        having :
          condition : {}

      expect(-> new sqlCompute.Having(command).validate()).toThrowError("The value of 'having.condition' should be of type Function")


  describe 'OrderBy Clause', ->

    it 'should raise an exception if value is not an Array ', ->
      command =
        orderBy : { not : 'an array' }

      expect(-> new sqlCompute.OrderBy(command).validate()).toThrowError("The value of 'orderBy' should be of type Array")


  describe 'Limit Clause', ->

    it 'should raise an exception if value is not a Number ', ->
      command =
        limit : "3"

      expect(-> new sqlCompute.Limit(command).validate()).toThrowError("The value of 'limit' should be of type Number")


  describe 'Offset Clause', ->

    it 'should raise an exception if value is not a Number ', ->
      command =
        offset : "3"

      expect(-> new sqlCompute.Offset(command).validate()).toThrowError("The value of 'offset' should be of type Number")
