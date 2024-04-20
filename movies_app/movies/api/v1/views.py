from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import FloatField, Q
from django.db.models.expressions import Case, When
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import Filmwork
from movies_app.settings import PAGINATE_BY


# not in mixins.py due to circular import
class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        return self.model.objects.values().annotate(
            genres=ArrayAgg(
                'genrefilmwork__genre__name',
                distinct=True,
                filter=Q(genrefilmwork__genre__name__isnull=False)),
            writers=ArrayAgg(
                'personfilmwork__person__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='writer')),
            actors=ArrayAgg(
                'personfilmwork__person__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='actor')),
            directors=ArrayAgg(
                'personfilmwork__person__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='director')),
            rating=Case(When(rating__isnull=True, then=0), default='rating', output_field=FloatField()),
        ).order_by('-rating')

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    def get_context_data(self, *, object_list=None, **kwargs):
        paginator, page, queryset, _ = self.paginate_queryset(
            self.get_queryset(),
            PAGINATE_BY,
        )

        context = {
            "results": list(queryset),
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": None if page.number == 1 else page.previous_page_number(),
            "next": None if page.number == paginator.num_pages else page.next_page_number(),
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, *, object_list=None, **kwargs):
        return self.object
