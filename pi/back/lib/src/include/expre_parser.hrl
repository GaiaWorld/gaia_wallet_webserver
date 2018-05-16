-define(LEFT_BRACKET, '(').
-define(RIGHT_BRACKET, ')').
-define(ADD, '+').
-define(SUB, '-').
-define(MUL, '*').
-define(DIV, '/').
-define(EDIV, 'div').
-define(REM, 'rem').
-define(POW, '^').
-define(LESS_THAN, '<').
-define(LESS_EQUAL_THAN, '=<').
-define(EQUAL, '=').
-define(GREATER_THAN, '>').
-define(GREATER_EQUAL_THAN, '>=').
-define(NO_EQUAL, '!=').
-define(AND, '&').
-define(OR, '|').
-define(NOT, '!').
-define(XOR, '~').
-define(OP_LIST, [
				  ?LEFT_BRACKET, ?RIGHT_BRACKET, ?ADD, ?SUB, ?MUL, ?DIV, ?EDIV, ?REM, ?POW, 
				  ?LESS_THAN, ?LESS_EQUAL_THAN, ?EQUAL, ?GREATER_THAN, ?GREATER_EQUAL_THAN, ?NO_EQUAL, 
				  ?AND, ?OR, ?NOT, ?XOR
				 ]).

-define(DEFAULT_OP_LEVEL_TABLE, [
								  {?ADD, 4}, 
								  {?SUB, 4}, 
								  {?MUL, 3}, 
								  {?DIV, 3}, 
								  {?EDIV, 3}, 
								  {?REM, 3}, 
								  {?POW, 2}, 
								  {?LESS_THAN, 5}, 
								  {?LESS_EQUAL_THAN, 5},
								  {?EQUAL, 5},
								  {?GREATER_THAN, 5}, 
								  {?GREATER_EQUAL_THAN, 5},
								  {?NO_EQUAL, 5},
								  {?AND, 3},
								  {?OR, 4},
								  {?NOT, 1},
								  {?XOR, 4},
								  {?LEFT_BRACKET, 6}
								]).

-define(DEFAULT_OP_META_TABLE, [
								  {?ADD, 2}, 
								  {?SUB, 2}, 
								  {?MUL, 2}, 
								  {?DIV, 2}, 
								  {?EDIV, 2}, 
								  {?REM, 2}, 
								  {?POW, 2}, 
								  {?LESS_THAN, 2}, 
								  {?LESS_EQUAL_THAN, 2},
								  {?EQUAL, 2},
								  {?GREATER_THAN, 2}, 
								  {?GREATER_EQUAL_THAN, 2},
								  {?NO_EQUAL, 2},
								  {?AND, 2},
								  {?OR, 2},
								  {?NOT, 1},
								  {?XOR, 2},
								  {?LEFT_BRACKET, 0}
								]).


