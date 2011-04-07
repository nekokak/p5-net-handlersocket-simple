package Net::HandlerSocket::Simple;
use strict;
use warnings;
use Net::HandlerSocket;
use Carp ();

our $VERSION = '0.01';

sub new {
    my ($class, $opts) = @_;

    bless +{
        client     => Net::HandlerSocket->new(+{host => $opts->{host}, port => $opts->{port}}),
        _index     => 0,
        _index_map => +{},
    }, $class;
}

sub select {
    my ($self, $db, $args, $opts) = @_;

    my $slice = $opts->{slice} || 'flat';
    if ($slice !~ /^(hash|array|flat)$/) {
        Carp::croak 'unknown slice: ' . $slice;
    }

    my $index = $self->_get_index($db, $opts->{'index'}, $args->{fields}, $opts->{filter});

    my $filters = $self->_make_filter_cond($opts->{filter});
    my ($where, $in_idx, $in_cond) = $self->_make_cond($args->{where});

    my $op     = $opts->{op}     || '=';
    my $limit  = $opts->{limit}  || 1;
    my $offset = $opts->{offset} || 0;

    my @exec_args = ($index, $op, $where, $limit, $offset);
    if ($filters) {
        push @exec_args, (undef, undef, $filters);
    }
    if ($in_cond) {
        push @exec_args, ($filters ? ($in_idx, $in_cond) : (undef,undef,undef, $in_idx, $in_cond));
    }
    use Data::Dumper;
    warn Dumper \@exec_args;
    
    my $dat = $self->{client}->execute_find(@exec_args);
    warn Dumper $dat;
    # FIXME: check error
    shift @$dat;

    return $dat if $slice eq 'flat';
    return $self->_response_filter($dat, $args->{fields}, $slice);
}

sub _get_index {
    my ($self, $db_table, $index, $fields, $filters) = @_;

    $index ||= 'PRIMARY';
    my ($db, $table) = split /\./, $db_table;
    my $fields_list  = (join ',', @$fields);
    my $filters_list = (join ',', keys %$filters);
    my $index_key = join '.', $db, $table, $index, $fields_list, $filters_list;

    $self->{_index_map}->{$index_key} ||= do {
        my $i = ++$self->{_index};
        my $rv = $self->{client}->open_index($i, $db, $table, $index, $fields_list, $filters_list);
        $i;
        # FIXME: check error
    };
}

sub _make_filter_cond {
    my ($self, $filter) = @_;

    my $filters;
    my $_filter_potition=0;
    my ($_filter_val, $_filter_op, $_filter_setting);
    for my $key (keys %$filter) {
        $_filter_setting = $filter->{$key};
        if (ref($_filter_setting) eq 'HASH') {
            ($_filter_op, $_filter_val) = each %$_filter_setting;
        }
        elsif (ref($_filter_setting) eq 'SCALAR') {
            ($_filter_op, $_filter_val) = ('=', $_filter_setting);
        }
        else {
            Carp::croak 'not allowed filter data format';
        }
        push @$filters, ['F', $_filter_op, $_filter_potition, $_filter_val];
        $_filter_potition++;
    }
    $filters;
}

sub _make_cond {
    my ($self, $where) = @_;

    my ($in_idx, $in_cond);
    my $where_count = scalar(@$where);

    for (my $i=0 ; $i < $where_count ; $i++) {
        next if ref($where->[$i]) ne 'ARRAY';
        die 'HandlerSocket IN specific for only one column' if $in_idx;
        $in_idx  = $i;
        $in_cond = $where->[$i];
        $where->[$i] = '';
    }
    ($where, $in_idx, $in_cond);
}

sub _response_filter {
    my ($self, $dat, $fields, $slice) = @_;

    my (@result, @_tmp);
    my $fields_count = scalar(@$fields);
    while (@_tmp = splice(@$dat, 0, $fields_count)) {
        if ($slice eq 'hash') {
            my $i=0;
            my %_dat = map {$_ => $_tmp[$i++]} @$fields;
            push @result, \%_dat;
        } elsif ($slice eq 'array') {
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

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
