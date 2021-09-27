<br/>

##`map`

<br/>
signature

###`map: (s: STREAM(T), fn: T => T1) => STREAM(T1)`
<br/>

`map` is a function that performs a scalar computation for each element in the given input stream `s` using the lambda `fn`.
While `map` consumes the stream `s` which is of type `T` element by element, `map` carries out a scalar computation
that results into an element of type `T1` that is then emitted into the resulting stream of type `T1`.

`map` is a function that is represented by a physical plan operator `RecordQueryMapPlan`.
<br/>

###Examples

```
scan 'RestaurantRecord' | map record => record.name
scan 'RestaurantRecord' | map _.name
scan 'RestaurantRecord' | map (_.name, _.rest_no) | map (name, _) => name
```

```
range(10) | map _ * 3
```

---------------

<br/>

##`range`

<br/>
signature

###`range: (exclusiveLimit: INT) => STREAM(INT)`
<br/>

`range` is a function that generates a stream that emits all integers from `0` up to `exclusiveLimit`.

`range` is a function that is represented by a physical plan operator `RecordQueryRangePlan`.
<br/>

###Examples

```
range(100)
range(10) | map _ * 3
```