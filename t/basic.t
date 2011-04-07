use strict;
use warnings;
use Test::More;
use DBI;
use Data::Dumper;

use Net::HandlerSocket::Simple;

my $dbh = DBI->connect('dbi:mysql:test','root','');

$dbh->do(q{
    CREATE TABLE IF NOT EXISTS handler_socket_test (
        id        INT auto_increment,
        name      TEXT,
        PRIMARY KEY  (id)
    )
});
$dbh->do(q{TRUNCATE TABLE handler_socket_test});
$dbh->do(q{
    INSERT INTO handler_socket_test (name) values ('nekokak'),('zigorou'),('xaicron'),('hidek')
});

my $rows = $dbh->selectall_arrayref('SELECT * FROM handler_socket_test', +{ Slice => +{} });
ok $rows;
note Dumper $rows;

my $hs = Net::HandlerSocket::Simple->new(+{host => '127.0.0.1', port => 9998});

my $dat = $hs->select(
    db      => 'test',
    table   => 'handler_socket_test',
    'index' => 'PRIMARY',
    op      => '=',
    fields  => [qw/id name/],
    where   => +[
        1,
    ],
    filter => +{
    },
    limit  => 4,
    offset => 0,
    slice  => 'flat',
);

ok $dat;

note Dumper $dat;

$dat = $hs->select(
    db      => 'test',
    table   => 'handler_socket_test',
    'index' => 'PRIMARY',
    op      => '=',
    fields  => [qw/id name/],
    where   => +[
        [qw/1 2 3/]
    ],
    filter => +{
    },
    limit  => 4,
    offset => 0,
    slice  => 'flat',
);

note Dumper $dat;

$dat = $hs->select(
    db      => 'test',
    table   => 'handler_socket_test',
    'index' => 'PRIMARY',
    op      => '=',
    fields  => [qw/id name/],
    where   => +[
        [qw/1 2 3/]
    ],
    filter => +{
    },
    limit  => 4,
    offset => 0,
    slice  => 'array',
);

note Dumper $dat;

$dat = $hs->select(
    db      => 'test',
    table   => 'handler_socket_test',
    'index' => 'PRIMARY',
    op      => '=',
    fields  => [qw/id name/],
    where   => +[
        [qw/1 2 3/]
    ],
    filter => +{
    },
    limit  => 4,
    offset => 0,
    slice  => 'hash',
);

note Dumper $dat;

done_testing;

