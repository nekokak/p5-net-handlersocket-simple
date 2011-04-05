use inc::Module::Install;
name 'Net-HandlerSocket-Simple';
all_from 'lib/Net/HandlerSocket/Simple.pm';

#requires '';

test_requires 'Test::More' => '0.94';

tests 't/*.t t/*/*.t';
author_tests('xt');

auto_set_repository;

WriteAll;
