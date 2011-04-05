package Net::HandlerSocket::Simple;
use strict;
use warnings;
use Net::HandlerSocket;
use Carp ();

our $VERSION = '0.01';

sub new {
    my ($class, $opts) = @_;

    bless +{
        handler_socket => Net::HandlerSocket->new(+{host => $opts->{host}, port => $opts->{port}}),
        _index         => 0,
        _index_map     => +{},
    }, $class;
}

sub install_index {
    my ($self, $index_name, $db, $table, $key, $columns) = @_;

    my $idx = ++$self->{_index};
    $self->{handler_socket}->open_index($idx, $db, $table, $key, join ',', @$columns);
    $self->{_index_map}->{$index_name} = +{
        'index' => $idx,
        col_num => scalar(@$columns),
        columns => $columns,
    };
}

sub find {
    my ($self, $index_name, $cond, $opts) = @_;

    $opts->{slice} ||= 'flat';
    $opts->{op}    ||= '=';

    if ($opts->{slice} !~ /^(hash|array|flat)$/) {
        Carp::croak 'unknown slice: ' . $opts->{slice};
    }

    my $_map = $self->{_index_map}->{$index_name};
    my $rows = $self->{handler_socket}->execute_find(
        $_map->{'index'},
        $opts->{op},
        $cond,
        ($opts->{limit}  || 0),
        ($opts->{offset} || 0),
    );
    shift @$rows; # index number

    return $rows if $opts->{slice} eq 'flat';

    my (@result, @_tmp);
    while (@_tmp = splice(@$rows, 0, $_map->{col_num})) {
        if ($opts->{slice} eq 'hash') {
            my $i=0;
            my %_dat = map {$_ => $_tmp[$i++]} @{$_map->{columns}};
            push @result, \%_dat;
        } elsif ($opts->{slice} eq 'array') {
            push @result, [@_tmp];
        }
    }

    \@result;
}

1;

__END__

=head1 NAME

Net::HandlerSocket::Simple - handler socket simple client

=head1 SYNOPSIS

    use Net::HandlerSocket::Simple;
    my $hs = Net::HandlerSocket::Simple->new(+{host => '127.0.0.1', port => 9998});
    $hs->install_index('index_name', 'test', 'handler_socket_test', 'PRIMARY', [qw/id name/]);
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0});
    # or
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0, slice => 'hash'});
    # or
    my $dat = $hs->find('index_name', [qw/1/], {op => '>=', limit => 4, offset => 0, slice => 'array'});

=head1 DESCRIPTION

Net::HandlerSocket::Simple is Net::HandlerSocekt simple wrapper

=head1 METHODS

=head2 my $hs = Net::HandlerSocket::Simple->new(\%options);

=over 4

=item * $options->{host}

handler socket daemon host.

=item * $options->{port}

handler socket daemon port.

=back

=head2 $hs->install_index($index_name, $db_name, $table_name, $key_name, \@get_columns_name);

=over 4

=item * $index_name

specific index name

=item * $db_name

mysql database name.

=item * $table_name

mysql table name

=item * $key_name

mysql key name

=item * \@get_columns_name

lookup columns name list

=back

=head2 my $row = $hs->find($index_name, \@cond, \%options);

=over 4

=item * $index_name

specific index name

=item * \@cond

key condition here

=item * $options->{op}

condition operator.

DEFAULT '='

=item * $options->{limit}

get limit record size.

DEFAULT 0

=item * $options->{offset}

offset record potition.

DEFAULT 0

=item * $options->{slice}

get data slice format.

ex) flat / array / hash

DEFAULT 'flat'

=back

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
