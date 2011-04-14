use strict;
use warnings;
use Test::More;
use DBI;

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
    INSERT INTO handler_socket_test (name) values ('nekokak')
});

my $rows = $dbh->selectall_arrayref('SELECT * FROM handler_socket_test', +{ Slice => +{} });
is_deeply $rows, [+{id => 1, name => 'nekokak'}];
note explain $rows;

my $hs = Net::HandlerSocket::Simple->new(+{host => '127.0.0.1', port => 9998, wo_host => '127.0.0.1', wo_port => '9999'});

my $res = $hs->update('test.handler_socket_test',
    +{
        where => +[1],
        set   => +{name => 'zigorou'},
    },
);

is $res, 1;
$rows = $dbh->selectall_arrayref('SELECT * FROM handler_socket_test', +{ Slice => +{} });
is_deeply $rows, [+{id => 1, name => 'zigorou'}];
note explain $rows;

done_testing;
