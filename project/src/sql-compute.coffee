###
    @author Piyush Katariya
###


# Exceptions

class DataFormatError extends Error

  constructor : (clause, requiredFormat) ->
    @message = "The value of '#{clause}' should be of type #{requiredFormat}"


class DependancyError extends Error

  constructor : (clause, requiredClause) ->
    @message = "'#{requiredClause}' is required if '#{clause}' is used"


class InvalidDataSourceAliasException extends Error

  constructor : (alias) ->
    @message = "Invalid database alias '#{alias}'"


# Data Source

class Iterator

  hasNext : ->
    false

  next : ->


class Promise

  then : (success) ->
    success()

  catch : (error) ->


class Resolver extends Promise

  constructor : (@promises...) ->
    @results = []
    @count = @promises.length
    @resolve = (index, allResolveComplete) =>
      (data) =>
        @result[index] = data
        if -- @count is 0
          allResolveComplete()

  then : (success, error) ->
    allResolveComplete = () ->
      success(@results...)

    for index, promise of @promises
      promise.then(@resolve(index, allResolveComplete))
      .catch(error)



class ArrayIterator extends Iterator

  constructor : (@array) ->
    @index = 0;
    @length = @array.length

  hasNext : ->
    @index < @length

  next : ->
    @array[@index++]


class DataSource extends Promise

  constructor : (@promise) ->

  then : (success) ->
    @promise.then (data) ->
      switch
        when data instanceof Array
          success(new ArrayIterator(data))
        when data instanceof Iterator
          success(data)
        else
          throw new DataFormatError 'datasource','either Array or instanceof Iterator class'


class ArrayDataSource extends Promise

  constructor : (@array) ->
    unless @array instanceof Array
      throw new DataFormatError 'ArrayDataSource', 'Array'

  then : (success)->
    success(@array) #sync because it is static data



# Query Engine


class OperationIterator extends Iterator

  constructor : (@operation, @iterator) ->


class SelectIterator extends Iterator

  hasNext : ->
    @iterator.hasNext()

  next : ->
    record = @iterator.next()
    for projection in @operation.projections
      [alias, property] = projection.split '.'


class FromIterator extends Iterator

  constructor : ()

  hasNext : ->


class Operation

  constructor : (@command) ->


class Select extends Operation

  validate : ->
    unless @command.select instanceof Array
      throw new DataFormatError 'select', 'Array'

    projections = (projection for projection in @command.select when typeof projection is 'string')
    ds = Object.keys @command.from
    for projection in projections
      [alias, _] = projection.split '.'
      throw new InvalidDataSourceAliasException alias if alias not in ds

  chain : (iterator)->

    new SelectIterator(@, iterator)


class From extends Operation

  constructor : (command) ->
    super command
    @promise = null

  validate : ->
    unless typeof @command.from is 'object'
      throw new DataFormatError 'from', 'JSON Object'

    for name, source of @command.from
      unless source instanceof DataSource
        throw new DataFormatError "datasource #{name}", 'either ArrayDataSource or CustomDataSource'

  chain : ->
    resolver = new Resolver((for ds in (@command.from alias for alias in Object.keys @command.from)))
    resolver.then()


class Where extends Operation

  validate : ->
    unless @command.where instanceof Function
      throw new DataFormatError 'where', 'Function'


class GroupBy extends Operation

  validate : ->
    unless @command.select instanceof Array
      throw new DataFormatError 'groupBy', 'Array'


class Having extends Operation

  validate : ->
    unless @command.groupBy
      throw new DependancyError 'having', 'groupBy'
    unless @command.having.condition
      throw new DependancyError 'having', 'having.condition'
    unless @command.having.condition instanceof Function
      throw new DataFormatError 'having.condition', 'Function'

class OrderBy extends Operation

  validate : ->
    unless @command.orderBy instanceof Array
      throw new DataFormatError 'orderBy', 'Array'


class Limit extends Operation

  validate : ->
    unless typeof @command.limit is 'number'
      throw new DataFormatError 'limit', 'Number'


class Offset extends Operation

  validate : ->
    unless typeof @command.offset is 'number'
      throw new DataFormatError 'offset', 'Number'


class Query

  constructor : (@command, ops) ->
    @pipeline = []
    @pipeline.push new ops.Where(@command) if @command.where
    @pipeline.push new ops.GroupBy(@command) if @command.groupBy
    @pipeline.push new ops.Having(@command) if @command.having
    @pipeline.push new ops.Select(@command)
    @pipeline.push new ops.OrderBy(@command) if @command.orderBy
    @pipeline.push new ops.Offset(@command) if @command.offset
    @pipeline.push new ops.Limit(@command) if @command.limit


  validate : ->
    unless typeof @command is 'object'
      throw new DataFormatError 'Query Object', 'JSON'
    unless @command.select and @command.from
      throw new DependancyError 'Query Object', '"select" and "from"'
    for operation in @pipeline
      operation.validate()


  execute : (success, error, lazy=false) ->
    try
      result = new ops.From(@command) #async
      result.promise.success = (dataIterator) ->
        try
          for operation in @pipeline #sync
            dataIterator = operation.chain(dataIterator)
          unless lazy
            records = []
            records.push dataIterator.next() while dataIterator.hasNext()
            success(data)
          else
            success(dataIterator)
        catch err
          error(err)
      result.promise.error = (err) -> error(err)
      result.execute()
    catch err
      error(err)


# Global exports

window.sqlCompute = window.sqlCompute ||
  Query : Query
  Select : Select
  From : From
  Where : Where
  GroupBy : GroupBy
  Having : Having
  OrderBy : OrderBy
  Limit : Limit
  Offset : Offset
  ArrayDataSource : (arr) -> new ArrayDataSource arr
  CustomDataSource : (fn) -> new CustomDataSource fn()
