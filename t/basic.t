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
$hs->install_index('index_name', 'test', 'handler_socket_test', 'PRIMARY', [qw/id name/]);

subtest 'find(flat)' => sub {
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0});
    note Dumper $dat;
    is_deeply $dat, [qw/1 nekokak 2 zigorou 3 xaicron 4 hidek/];
};

subtest 'find(array)' => sub {
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0, slice => 'array'});
    note Dumper $dat;
    is_deeply $dat, [[qw/1 nekokak/],[qw/2 zigorou/],[qw/3 xaicron/],[qw/4 hidek/]];
};

subtest 'find(flat)' => sub {
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0, slice => 'hash'});
    note Dumper $dat;
    is_deeply $dat, [+{id => 1, name => 'nekokak'}, +{id => 2, name => 'zigorou'}, +{id => 3, name => 'xaicron'}, +{id => 4, name => 'hidek'}];
};

done_testing;

