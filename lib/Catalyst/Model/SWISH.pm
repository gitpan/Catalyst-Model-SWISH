use strict;
use warnings;
package Catalyst::Model::SWISH;
use base 'Catalyst::Model';
use Carp;
use SWISH::API::Object;
use NEXT;
use Data::Pageset;
use Time::HiRes;
use Sort::SQL;

our $VERSION = '0.02';

__PACKAGE__->mk_accessors(qw( swish swish_error context ));

=head1 NAME

Catalyst::Model::SWISH - Catalyst model for Swish-e

=head1 SYNOPSIS

 # in your Controller
 sub search : Local
 {
    my ($self,$c) = @_;
    my ($pager,$results,$query,$order,$total,$stime,$btime) = 
     $c->model('SWISH')->search(
      query         => $c->request->param('q'),
      page          => $c->request->param('page') || 0,
      page_size     => $c->request->param('itemsperpage') || 0,
      limit_to      => 'swishtitle',
      limit_high    => 'm',
      limit_low     => 'a',
      order_by      => 'swishrank desc swishtitle asc'
     );
    $c->stash->{search} = {
                           results  => $results,
                           pager    => $pager,
                           query    => $query,
                           order    => $order,
                           hits     => $total,
                           search_time => $stime,
                           build_time  => $btime
                          };
 }
 

=head1 DESCRIPTION

Catalyst::Model::SWISH provides an easy interface to 
SWISH::API::Object for searching
Swish-e indexes (http://www.swish-e.org/). It is similar to and inspired by
Catalyst::Model::Xapian.

=head1 CONFIGURATION

The configuration value is passed directly to SWISH::API::Object->new()
so see the SWISH::API::Object documentation for possible key/value options.

=over

=item indexes

Path(s) to the Swish-e index(es). Defaults to <MyApp>/index.swish-e.

=item page_size

Default page sizes for L<Data::Pageset>. Defaults to 10.

=back

You may set config options either in your root class config() method prior to
setup(), using your Model class name as the key, or set them directly in
your Model class config() method.

Examples:

 # in MyApp.pm
 MyApp->config(
    'MyApp::Model::SWISH' => { 
        'indexes' => MyApp->path_to('some/where/index.swish-e')->stringify
        }
    );
    
 # same thing in MyApp/Model/SWISH.pm
 MyApp::Model::SWISH->config(
    'indexes' => MyApp->path_to('some/where/index.swish-e')->stringify
    );
 


=head1 METHODS

=head2 new

Constructor is called automatically by Catalyst at startup.

=cut


sub ACCEPT_CONTEXT
{
    my ($self, $c, @args) = @_;
    my $new = bless({%$self}, ref $self);
    $new->context($c);
    return $new;
}


sub new
{
    my ($class, $c) = @_;
    my $self = $class->NEXT::new($c);
    my %config = (
                  indexes => $self->config->{indexes}
                    || $c->config->{$class}->{indexes}
                    || $c->config->{home} . '/index.swish-e',
                  pages_per_set => $self->config->{pages_per_set} || 10,
                  %{$self->config()},
                  %{$c->config->{$class}},
                 );

    # page_size == 0 is a valid value, so we must use defined()
    unless (defined $config{page_size})
    {
        if (defined $self->config->{page_size})
        {
            $config{page_size} = $self->config->{page_size};
        }
        elsif (defined $c->config->{$class}->{page_size})
        {
            $config{page_size} = $c->config->{$class}->{page_size};
        }
        else
        {
            $config{page_size} = 10;
        }
    }

    $self->config(\%config);
    $self->connect;
    return $self;
}

=head2 search( I<opts> )

Perform a search on the index.

Returns (in order):

=over

=item 

a L<Data::Pageset> object

=item

an arrayref of SWISH::API::Result objects.

=item

an arrayref of parsed query terms

=item

an arrayref of property sort order, where each array item is a hashref like:

 { property => asc | desc }

=item

the total number of hits

=item

the search time

=item

the build time

=back


I<opts> require a C<query> name/value pair at minimum. Other
valid params include:

=over

=item page

Which page to start on. Used in cooperation with C<page_size> set in new().

=item order_by

Sort results by a property other than rank.

=item limit_to

Property name to limit results by.

=item limit_high

High value for C<limit_to>.

=item limit_low

Low value for C<limit_to>.

=back

=cut

sub search
{
    my $self = shift;
    my $c    = $self->context;
    my %opts = @_;

    $opts{query} or croak "query required";
    $opts{page}          ||= 1;
    $opts{page_size} = defined($opts{page_size}) ?
        $opts{page_size} : $self->config->{page_size};
    $opts{pages_per_set} ||= $self->config->{pages_per_set};

    my $start_time = [Time::HiRes::gettimeofday()];
    my $search     = $self->swish->new_search_object;
    if ($self->_check_err)
    {
        $c->error($self->swish_error);
        return;
    }

    if ($opts{limit_to})
    {
        defined $opts{limit_high} or croak "limit_high required with limit_to";
        defined $opts{limit_low}  or croak "limit_low required with limit_to";

        $search->set_search_limit($opts{limit_to}, $opts{limit_low},
                                  $opts{limit_high});
        if ($self->_check_err)
        {
            $c->error($self->swish_error);
            return;
        }
    }
    if ($opts{order_by})
    {
        $search->set_sort($opts{order_by});
    }

    my $results = $search->execute($opts{query});
    if ($self->_check_err)
    {
        $c->error($self->swish_error);
        return;
    }
    my $search_time = sprintf(
                              '%0.4f',
                              Time::HiRes::tv_interval(
                                      $start_time, [Time::HiRes::gettimeofday()]
                              )
                             );

    my @r;
    my $start       = ($opts{page} - 1) * $opts{page_size};
    my $build_start = [Time::HiRes::gettimeofday()];
    $results->seek_result($start) unless $start > $results->hits;
    my $count = 0;
    while (my $r = $results->next_result)
    {
        push(@r, $r);
        if (++$count >= $opts{page_size} && $opts{page_size} != 0)
        {
            last;
        }
    }

    my $pager;

    unless ($opts{page_size} == 0)
    {

        $pager =
          Data::Pageset->new(
                             {
                              total_entries    => $results->Hits,
                              entries_per_page => $opts{page_size},
                              current_page     => $opts{page},
                              pages_per_set    => $opts{pages_per_set},
                              mode             => 'slide',
                             }
                            );

    }

    my $build_time = sprintf(
                             '%0.4f',
                             Time::HiRes::tv_interval(
                                      $start_time, [Time::HiRes::gettimeofday()]
                             )
                            );

    return (
            $pager,
            \@r,
            [$results->parsed_words($self->swish->indexes->[0])],
            Sort::SQL->string2array($opts{order_by} || 'swishrank desc'),
            $results->hits,
            $search_time,
            $build_time
           );
}

sub _check_err
{
    my $self = shift;

    if ($self->swish->error)
    {
        $self->swish_error(
                               ref($self) . ": "
                             . $self->swish->error_string . ": "
                             . $self->swish->last_error_msg);
        return 1;
    }
    return 0;
}

=head2 connect

Calling connect() will DESTROY the cached SWISH::API::Object object and re-cache
it, essentially re-opening the Swish-e index.

B<NOTE:> SWISH::API::Object actually makes this unnecessary in most cases,
since it inherits from SWISH::API::Stat.

=cut

sub connect
{
    my $self = shift;
    $self->{swish} = SWISH::API::Object->new(%{$self->config});
    croak $self->swish_error if $self->_check_err;

    # use RankScheme 1 if the index supports it
    my $rs =
      $self->swish->header_value($self->swish->indexes->[0],
                                 'IgnoreTotalWordCountWhenRanking');
    if (!$rs)
    {
        $self->swish->rank_scheme(1);
    }
}


1;

__END__

=head1 AUTHOR

Peter Karman <perl@peknet.com>

Thanks to Atomic Learning, Inc for sponsoring the development of this module.

=head1 LICENSE

This library is free software. You may redistribute it and/or modify it under
the same terms as Perl itself.


=head1 SEE ALSO

http://www.swish-e.org/, SWISH::API::Object

=cut

